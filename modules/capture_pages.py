"""Authenticated capture-position preview and one-time application."""

import json

from flask import g, jsonify, redirect, render_template, request, url_for

from . import capture_requests
from . import repository as repo
from .binlog_position import resolve_datetime, resolve_gtid
from .capture_history import schema_baseline
from .capture_start import selection, validate_gtid_boundary
from .heatwave import connect


def require_control(cloud, flow):
    if not flow.get("connector_ocid") or flow["status"] == "retired":
        raise ValueError(
            "Deploy a non-retired connector before changing an existing capture position."
        )
    instance = cloud.owned_container(flow["connector_ocid"], flow["id"])
    if (instance.freeform_tags or {}).get("capture-control-protocol") != "3":
        raise ValueError(
            "Build and select the current Debezium image, then use Update Debezium image. This deployed image does not support capture-position requests."
        )
    if instance.lifecycle_state != "ACTIVE":
        raise ValueError(
            "Start the Debezium instance before submitting a position request. The request stops and resumes the connector task safely inside it."
        )


def plan_position(cloud, flow, choice):
    baseline = schema_baseline(flow, cloud)
    with connect(flow["source"], cloud) as source:
        if choice["mode"] == "datetime":
            boundary = resolve_datetime(flow, cloud, baseline, choice["value"])
            requested = boundary["gtids"]
        elif choice["mode"] == "end":
            requested = repo.query(source, "SELECT @@GLOBAL.gtid_executed AS value", one=True)[
                "value"
            ]
        elif choice["mode"] == "start":
            requested = baseline["gtids"]
        else:
            boundary = resolve_gtid(flow, cloud, baseline, choice["value"])
            requested = boundary["gtids"]
        requested = validate_gtid_boundary(source, requested, baseline["gtids"])
    position = boundary if choice["mode"] in {"gtid", "datetime"} else baseline
    result = {
        "partition": {"server": "audit_" + str(flow["id"])},
        "offset": {
            "file": position["file"],
            "pos": position["pos"],
            "gtids": requested,
            "row": 0,
            "event": 0,
        },
    }
    if choice["mode"] == "gtid":
        result["source_range"] = boundary["source_range"]
    return result, baseline


def register(app, protected, cloud_factory, notify):
    @app.post("/configuration/gtid-check")
    @protected(admin=True)
    def initial_gtid_check():
        from .gtid_position import locate_gtid, source_range

        source = repo.query(
            g.db, "SELECT * FROM connections WHERE id=%s AND role='source'",
            (request.form.get("source_id"),), one=True,
        )
        if not source:
            raise ValueError("Choose a registered source connection first.")
        cloud = cloud_factory()
        with connect(source, cloud) as db:
            limits = source_range(db)
            try:
                choice = selection({"mode": "gtid", "value": request.form.get("gtid")})
                boundary = locate_gtid(db, choice["value"], check_schema=True)
            except ValueError as error:
                return jsonify(ok=False, message=str(error), source_range=limits), 400
        return jsonify(ok=True, boundary=boundary)

    @app.route("/flows/<int:flow_id>/capture-start", methods=["GET", "POST"])
    @protected(admin=True)
    def capture_start(flow_id):
        flow = repo.get_flow(g.db, flow_id)
        choice = selection(flow.get("capture_start"))
        preview, last, message = None, None, None
        cloud = cloud_factory()
        from .gtid_position import source_range

        try:
            with connect(flow["source"], cloud) as source:
                gtid_range = source_range(source)
        except Exception:
            gtid_range = None
        if request.method == "POST":
            action = request.form.get("action")
            with repo.named_lock(g.db, "flow", flow_id):
                flow = repo.get_flow(g.db, flow_id)
                if flow["status"] == "retired":
                    raise ValueError("Retired flows cannot change capture position.")
                if action == "save-initial":
                    if flow.get("connector_ocid") or flow.get("processor_ocid") or flow["status"] != "draft":
                        raise ValueError(
                            "This flow already has a connector. Preview an explicit position change instead."
                        )
                    choice = selection({"mode": request.form.get("mode"), "value": request.form.get("gtid")})
                    if choice["mode"] not in {"start", "end", "gtid"}:
                        raise ValueError(
                            "Choose Start, End or Specific GTID for initial capture."
                        )
                    if choice["mode"] == "gtid":
                        from .gtid_position import locate_gtid

                        try:
                            with connect(flow["source"], cloud) as source:
                                locate_gtid(source, choice["value"], check_schema=True)
                        except ValueError as error:
                            return render_template(
                                "capture_start.html", flow=flow, choice=choice, preview=None,
                                last=None, message=str(error), gtid_range=gtid_range,
                            ), 400
                    repo.migrate_resources(g.db)
                    repo.query(
                        g.db,
                        "UPDATE flows SET capture_start=%s WHERE id=%s",
                        (json.dumps(choice), flow_id),
                    )
                    repo.record(g.db, flow_id, "capture-initial", "saved", g.record["username"])
                    notify(
                        "Initial capture choice saved. Normal restarts continue from existing offsets.",
                        "success",
                    )
                    return redirect(url_for("capture_start", flow_id=flow_id))
                require_control(cloud, flow)
                if action == "preview":
                    mode = request.form.get("mode")
                    choice = selection(
                        {
                            "mode": mode,
                            "value": request.form.get("gtid" if mode == "gtid" else "datetime"),
                        }
                    )
                    try:
                        plan, baseline = plan_position(cloud, flow, choice)
                    except ValueError as error:
                        return render_template(
                            "capture_start.html", flow=flow, choice=choice, preview=None,
                            last=None, message=str(error), gtid_range=gtid_range,
                        ), 400
                    with connect(flow["target"], cloud) as db:
                        request_id = capture_requests.enqueue(
                            db, flow, choice, plan, g.record["username"], preview=True
                        )
                    preview = {
                        "request_id": request_id,
                        "plan": plan,
                        "baseline": baseline,
                        "choice": choice,
                    }
                elif action == "apply":
                    if request.form.get("acknowledge") != "yes":
                        raise ValueError(
                            "Acknowledge replay or skipped coverage before applying the preview."
                        )

                    def revalidate(plan):
                        baseline = schema_baseline(flow, cloud)
                        with connect(flow["source"], cloud) as source:
                            validate_gtid_boundary(
                                source, plan["offset"]["gtids"], baseline["gtids"]
                            )

                    with connect(flow["target"], cloud) as db:
                        capture_requests.confirm(
                            db,
                            flow,
                            request.form.get("request_id", ""),
                            g.record["username"],
                            revalidate,
                        )
                    repo.record(
                        g.db, flow_id, "capture-position", "submitted", g.record["username"]
                    )
                    notify(
                        "Capture position request submitted. The connector stops, applies the reviewed boundary once, then resumes. Refresh this page to check its result.",
                        "success",
                    )
                    return redirect(url_for("capture_start", flow_id=flow_id))
                elif action == "resume-checkpoint":
                    with connect(flow["target"], cloud) as db:
                        capture_requests.enqueue(
                            db, flow, {"mode": "resume"}, {"action": "resume"}, g.record["username"]
                        )
                    repo.record(g.db, flow_id, "capture-resume", "submitted", g.record["username"])
                    notify(
                        "Connector resume requested using its existing checkpoint. No position reset was requested.",
                        "success",
                    )
                    return redirect(url_for("capture_start", flow_id=flow_id))
                else:
                    raise ValueError("Unknown capture-position action.")
        if flow.get("connector_ocid"):
            try:
                with connect(flow["target"], cloud) as db:
                    last = capture_requests.latest(db, flow)
            except Exception as error:
                if getattr(error, "errno", None) == 1146:
                    message = "This connector predates capture-position control. Update to a newly built Debezium image before using historical positioning."
                else:
                    raise
        return render_template(
            "capture_start.html",
            flow=flow,
            choice=choice,
            preview=preview,
            last=last,
            message=message,
            gtid_range=gtid_range,
        )
