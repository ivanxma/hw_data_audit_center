"""Loopback-only presentation server. Uses fixture data, never OCI or live databases."""

# Repository imports follow the checkout-relative sys.path bootstrap.
# ruff: noqa: E402

import json
import secrets
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEMO = ROOT / "demo"
sys.path.insert(0, str(ROOT))
from flask import Flask, render_template, request, send_from_directory

from app import app as application
from modules.events import decode
from modules.resource_catalog import SERVICES, TABS
from modules.resource_forms import FORMS
from modules.schema_history import difference

app = Flask(
    "audit_video_preview",
    template_folder=str(ROOT / "templates"),
    static_folder=str(ROOT / "static"),
)
app.secret_key = secrets.token_bytes(32)
app.config["TEMPLATES_AUTO_RELOAD"] = True
F = dict(
    id=1,
    name="orders-history",
    source_id=1,
    target_id=2,
    source_name="Sales source",
    target_name="History target",
    source_schema="demo_sales",
    source_table="orders",
    target_schema="demo_audit",
    target_table="demo_sales_orders_history",
    topic="demo-orders",
    transport="streaming",
    connector_name="demo-orders-connector",
    status="active",
    processor_ocid="demo-writer",
    connector_ocid="demo-reader",
    queue_connection_id=1,
    capture_start='{"mode":"end"}',
    updated_at=datetime(2026, 10, 24, 10, 14),
    last_action="deploy",
    source_identity="illustrative-source-identity",
    lineage_id="illustrative-connector",
    queue_incarnation="illustrative-stream-pool",
)
C = [
    dict(
        id=1,
        name="Sales source",
        role="source",
        host="source.example.invalid",
        port=3306,
        username="cdc_reader",
        database_name="demo_sales",
        secret_ocid="ocid1.vaultsecret.example.source",
        server_identity=F["source_identity"],
    ),
    dict(
        id=2,
        name="History target",
        role="target",
        host="target.example.invalid",
        port=3306,
        username="history_writer",
        database_name="demo_audit",
        secret_ocid="ocid1.vaultsecret.example.target",
        server_identity="illustrative-target-identity",
    ),
]
Q = [
    dict(
        id=1,
        name="Orders stream pool",
        kind="oci_streaming",
        resource_ocid="demo-pool",
        username="",
        secret_ocid="",
    )
]
created = dict(id=1042, status="new", amount="80.00")
updated = dict(id=1042, status="paid", amount="99.95")
EVENTS = []
ENVELOPES = []
for i, (op, before, after) in enumerate(
    [("c", None, created), ("u", created, updated), ("d", updated, None)]
):
    env = dict(
        op=op,
        before=before,
        after=after,
        source=dict(
            db="demo_sales",
            table="orders",
            file="binlog.000017",
            pos=2048 + i * 512,
            row=0,
            ts_ms=int(datetime(2026, 10, 24, 10, 14 + i, tzinfo=timezone.utc).timestamp() * 1000),
            snapshot=False,
        ),
    )
    ENVELOPES.append(env)
    EVENTS.append(
        decode(
            json.dumps(env),
            b'{"id":1042}',
            F["topic"],
            0,
            100 + i,
            F,
            b"public-illustration-key-not-for-production",
        )
    )
assert len({e["key_hash"] for e in EVENTS}) == 1
BASE = {
    "columns": {
        "id": {"typeName": "BIGINT", "optional": False},
        "status": {"typeName": "VARCHAR", "length": 20, "defaultValueExpression": "'new'"},
        "amount": {
            "typeName": "DECIMAL",
            "length": 10,
            "scale": 2,
            "defaultValueExpression": "0.00",
        },
    },
    "primaryKeyColumnNames": ["id"],
}
AFTER = json.loads(json.dumps(BASE))
AFTER["columns"]["region"] = {
    "typeName": "VARCHAR",
    "length": 16,
    "optional": True,
    "defaultValueExpression": "'EMEA'",
}
AFTER["columns"]["amount"]["length"] = 14
DATA = {
    "events": EVENTS,
    "envelopes": ENVELOPES,
    "key_hash": EVENTS[0]["key_hash"],
    "before_definition": BASE,
    "after_definition": AFTER,
    "semantic_delta": difference(BASE, AFTER),
}


@app.route("/movie")
def movie():
    return send_from_directory(DEMO / "source", "stage.html")


@app.route("/demo-files/<path:name>")
def files(name):
    return send_from_directory(DEMO, name)


@app.route("/demo-data")
def fixture_data():
    return app.json.response(DATA)


@app.context_processor
def context():
    return dict(
        csrf="illustrative-form-token",
        current_username="Demo operator",
        is_admin=True,
        app_version="0.1.0",
        current_profile_name="Demo workspace",
        active_menu=request.endpoint,
        page_title={
            "audit": "Audit Center",
            "configuration": "Configuration & setup",
            "messages": "Message Explorer",
            "control": "Control Center",
            "dashboard": "Dashboard",
        }.get(request.endpoint, "HeatWave Audit"),
        app_title="HeatWave Audit",
        auth_shell=False,
        services=SERVICES,
        service_tabs=TABS,
        resource_forms=FORMS,
        queue_connections=Q,
        runtime_configuration={},
    )


def render_page(endpoint, **kwargs):
    if endpoint == "notifications":
        return dict(items=[])
    if endpoint == "flow_status":
        return dict(
            id=kwargs.get("flow_id", 1),
            status="active",
            workloads=[
                dict(
                    role=role,
                    state="ACTIVE",
                    process="RUNNING",
                    task_state="RUNNING" if role == "Debezium" else None,
                    update_available=False,
                    guidance="Illustrative healthy workload status.",
                )
                for role in ["Debezium", "History writer"]
            ],
        )
    if endpoint == "initial_gtid_check":
        return {
            "ok": True,
            "boundary": {
                "source_range": {
                    "executed": "11111111-1111-1111-1111-111111111111:1-32:40-45,22222222-2222-2222-2222-222222222222:1-18",
                    "purged": "11111111-1111-1111-1111-111111111111:1-20",
                }
            },
        }
    if endpoint == "resource_scope":
        return dict(
            compartments=[dict(id="demo-compartment", name="VM compartment (demo)")],
            selected="demo-compartment",
            region="Example OCI region",
        )
    if endpoint == "service_inventory":
        rows = {
            "stream_pool": [
                dict(id="demo-pool", name="Orders stream pool", state="ACTIVE", managed=True)
            ],
            "stream": [
                dict(id="demo-data-stream", name="demo-orders", state="ACTIVE", managed=True),
                dict(id="demo-schema-stream", name="audit_1", state="ACTIVE", managed=True),
            ],
        }
        return dict(resources=rows.get(kwargs.get("kind"), []))
    if endpoint == "service_options":
        opts = {
            "stream_pool": [dict(id="demo-pool", name="Orders stream pool", state="ACTIVE")],
            "secret": [
                dict(
                    id="ocid1.vaultsecret.example.source",
                    name="Demo source password reference",
                    state="ACTIVE",
                )
            ],
            "availability_domain": [dict(id="Example AD", name="Example AD")],
        }
        return dict(options=opts.get(kwargs.get("kind"), []))
    if endpoint == "queue_topics":
        return dict(
            topics=[
                dict(name="demo-orders", partitions=1, state="ACTIVE"),
                dict(name="audit_1", partitions=1, state="ACTIVE"),
            ]
        )
    if endpoint == "connection_objects":
        if request.args.get("schema"):
            return dict(
                tables=[
                    dict(
                        name="orders"
                        if kwargs["connection_id"] == 1
                        else "demo_sales_orders_history",
                        compatible=True,
                    )
                ]
            )
        return dict(schemas=["demo_sales" if kwargs["connection_id"] == 1 else "demo_audit"])
    if endpoint == "test_connection":
        return dict(
            ok=True,
            message="Illustrative test result: connected with TLS. No live database was contacted.",
        )
    flows = [F]
    ctx = dict(
        flows=flows,
        connections=C,
        queue_connections=Q,
        operations=[],
        connection_count=2,
        resources={},
        provisioned=[],
        configured=True,
        setup_sql="CREATE DATABASE IF NOT EXISTS audit_control;",
        flow=F,
        records=[],
    )
    if endpoint == "configuration":
        if request.args.get("edit"):
            from modules.configuration_entries import FIELDS, row_values, version

            kind = request.args["edit"]
            row = C[0] if kind == "connection" else F if kind == "flow" else Q[0]
            ctx["edit_entry"] = dict(
                kind=kind,
                id=row["id"],
                values=row_values(kind, row),
                version=version(kind, row),
                locked=[n for n in FIELDS[kind] if n != "name"],
                message="This demonstration flow has deployment history. Only its name is editable.",
                url="/demo-save",
            )
    elif endpoint == "messages":
        idx = int(request.args.get("event", "0"))
        ctx["records"] = [
            dict(
                partition=0,
                offset=100 + idx,
                topic=F["topic"],
                key='{"id":1042}',
                value=ENVELOPES[idx],
            )
        ]
    elif endpoint == "audit":
        if request.args.get("tab") == "audit-global" or not request.args.get("flow_id"):
            flows = [
                F,
                {**F, "id": 2, "name": "invoice-history", "source_table": "invoices"},
                {
                    **F,
                    "id": 3,
                    "name": "stock-history",
                    "source_id": 3,
                    "source_name": "Inventory source",
                    "source_schema": "demo_inventory",
                    "source_table": "stock",
                },
            ]
            ctx["flows"] = flows
            rows = [
                dict(
                    flow_id=f["id"],
                    source_id=f["source_id"],
                    database=f["source_name"] + " / " + f["source_schema"],
                    schema=f["source_schema"],
                    label=f["source_schema"] + "." + f["source_table"],
                    count=n,
                    state="available",
                )
                for f, n in zip(flows, [549, 230, 300])
            ]
            if request.args.get("source_id"):
                rows = [r for r in rows if str(r["source_id"]) == request.args["source_id"]]
            ctx["overview"] = dict(
                rows=rows,
                total=sum(r["count"] for r in rows),
                unavailable=0,
                **{"from": "2026-10-17T00:00:00", "to": "2026-10-24T00:00:00"},
            )
            return render_template("audit_global.html", **ctx)
        rows = list(reversed(EVENTS[: int(request.args.get("event", "2")) + 1]))
        from modules.audit_service import chart_window

        start, end, interval = chart_window(
            {
                **request.args.to_dict(),
                "from": request.args.get("from", "2026-10-24T10:00:00"),
                "to": request.args.get("to", "2026-10-24T11:00:00"),
            }
        )
        epoch = int(start.replace(tzinfo=timezone.utc).timestamp())
        stop = int(end.replace(tzinfo=timezone.utc).timestamp())
        first = epoch // interval * interval
        chart = [dict(bucket=first, operation=r["operation"], n=1) for r in rows]
        if request.args.get("sample") == "charts" or request.args.get("tab") == "audit-trends":
            chart = [
                dict(bucket=b, operation=op, n=(i * 7 + j * 13) % 45 + 5)
                for i, b in enumerate(range(first, stop, interval))
                for j, op in enumerate(["CREATE", "UPDATE", "DELETE"])
            ]
        counts = [
            dict(operation=op, n=sum(c["n"] for c in chart if c["operation"] == op))
            for op in ["CREATE", "UPDATE", "DELETE"]
        ]
        ctx.update(
            result=dict(
                rows=rows,
                total=sum(r["n"] for r in counts),
                page=1,
                chart=chart,
                counts=counts,
                interval=interval,
                chart_start=epoch,
                chart_end=stop,
                **{"from": start.isoformat(), "to": end.isoformat()},
            ),
            export_args=dict(flow_id=1, export="csv"),
            prev_args={},
            next_args={},
        )
    elif endpoint == "capture_start":
        ctx.update(choice={"mode": "end"}, preview=None, last=None, message=None, gtid_range=None)
        return render_template("capture_start.html", **ctx)
    elif endpoint not in {"dashboard", "control"}:
        return "This route is outside the illustrative demonstration.", 404
    return render_template(endpoint + ".html", **ctx)


# Same URL names as the app, with a separate loopback fixture server and no live handlers.
for rule in application.url_map.iter_rules():
    if rule.endpoint == "static":
        continue

    def handler(_endpoint=rule.endpoint, **kwargs):
        return render_page(_endpoint, **kwargs)

    app.add_url_rule(
        rule.rule,
        endpoint=rule.endpoint,
        view_func=handler,
        methods=list(rule.methods - {"HEAD", "OPTIONS"}),
    )


@app.after_request
def preview_headers(response):
    response.headers["Cache-Control"] = "no-store"
    return response


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5155, threaded=True)
