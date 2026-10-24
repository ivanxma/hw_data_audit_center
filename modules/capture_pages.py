"""Authenticated capture-position preview and one-time application."""

import json

from flask import g, redirect, render_template, request, url_for

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
    if (instance.freeform_tags or {}).get("capture-control-protocol") != "2":
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
            requested = resolve_gtid(flow, cloud, baseline, choice["value"])["gtids"]
        requested = validate_gtid_boundary(source, requested, baseline["gtids"])
    return {
        "partition": {"server": "audit_" + str(flow["id"])},
        "offset": {
            "file": baseline["file"],
            "pos": baseline["pos"],
            "gtids": requested,
            "row": 0,
            "event": 0,
        },
    }, baseline


def register(app, protected, cloud_factory, notify):
    @app.route("/flows/<int:flow_id>/capture-start", methods=["GET", "POST"])
    @protected(admin=True)
    def capture_start(flow_id):
        flow = repo.get_flow(g.db, flow_id)
        choice = selection(flow.get("capture_start"))
        preview, last, message = None, None, None
        cloud = cloud_factory()
        if request.method == "POST":
            action = request.form.get("action")
            with repo.named_lock(g.db, "flow", flow_id):
                flow = repo.get_flow(g.db, flow_id)
                if flow["status"] == "retired":
                    raise ValueError("Retired flows cannot change capture position.")
                if action == "save-initial":
                    if flow.get("connector_ocid"):
                        raise ValueError(
                            "This flow already has a connector. Preview an explicit position change instead."
                        )
                    choice = selection({"mode": request.form.get("mode")})
                    if choice["mode"] not in {"start", "end"}:
                        raise ValueError(
                            "A new flow needs Start or End to establish schema history before historical positioning."
                        )
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
                    plan, baseline = plan_position(cloud, flow, choice)
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
        )
