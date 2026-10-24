#!/usr/bin/env python3
"""HeatWave Audit Center: socket-authenticated control plane on an OCI VM."""

import csv
import io
import json
import os
import secrets
import threading
import time
from functools import wraps

from flask import (
    Flask,
    Response,
    abort,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from flask import flash as flask_flash

from modules import repository as repo
from modules.audit_service import browse, overview
from modules.config import ROOT, load_config, local_profile, write_private_json
from modules.heatwave import connect, validate_source
from modules.mysql_util import ConnectionCache
from modules.oci_service import Cloud
from modules.queue_service import explore
from modules.session_store import ServerSessionStore
from modules.validation import bounded

SESSIONS = ServerSessionStore(ttl_seconds=3600)
CONNECTIONS = ConnectionCache()
LOCK = threading.RLock()
ATTEMPTS = {}


def notify(message, category="info", event_key=None):
    record = getattr(g, "record", {})
    SESSIONS.notify(record.get("connection_id"), message, category, event_key)


def flash(message, category="message"):
    flask_flash(message, category)
    notify(message, category)


def key():
    path = ROOT / ".flask_secret_key"
    if not path.exists():
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(secrets.token_hex(32))
    return path.read_text().strip()


def create_app(test_config=None):
    app = Flask(__name__)
    app.config.update(
        SECRET_KEY=key(),
        SESSION_COOKIE_NAME="heatwave_audit",
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=os.environ.get("AUDIT_SESSION_COOKIE_SECURE", "1") == "1",
        MAX_CONTENT_LENGTH=1024 * 1024,
    )
    if test_config:
        app.config.update(test_config)

    def clear():
        sid = session.get("sid")
        CONNECTIONS.close(sid)
        SESSIONS.clear(sid)
        session.clear()

    @app.before_request
    def prepare():
        if request.endpoint in {"health", "static"}:
            g.record = {}
            return
        # Serialize a cached connection per session, without blocking other users
        # behind slow OCI calls, target queries or partition maintenance.
        with LOCK:
            for sid in SESSIONS.prune_expired():
                CONNECTIONS.close(sid)
            record = SESSIONS.get(session.get("sid"))
            if not record:
                session.clear()
                session["sid"] = SESSIONS.create("", "", "", {})
                record = SESSIONS.get(session["sid"])
        g.request_lock = record["request_lock"]
        g.request_lock.acquire()
        record = SESSIONS.get(session.get("sid"))
        if not record:
            session.clear()
            return redirect(url_for("login"))
        g.record = record
        if request.method == "POST" and not secrets.compare_digest(
            request.form.get("csrf", ""), record["csrf"]
        ):
            abort(400, "Invalid form token. Reload the page.")

    @app.teardown_request
    def release(_error):
        lock = g.pop("request_lock", None)
        if lock is not None:
            lock.release()

    @app.after_request
    def headers(response):
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"
        )
        if app.config["SESSION_COOKIE_SECURE"]:
            response.headers["Strict-Transport-Security"] = "max-age=31536000"
        return response

    def protected(admin=False):
        def decorator(view):
            @wraps(view)
            def wrapped(*args, **kwargs):
                record = g.record
                if not record.get("username"):
                    return redirect(url_for("login"))
                try:
                    g.db = CONNECTIONS.borrow(
                        record["connection_id"],
                        record["profile"],
                        record["username"],
                        record["password"],
                    )
                except Exception:
                    clear()
                    return redirect(url_for("login"))
                if rotation_required(record["username"]) and request.endpoint not in {
                    "password",
                    "logout",
                }:
                    return redirect(url_for("password"))
                if admin and record["username"] not in admin_users():
                    abort(403)
                return view(*args, **kwargs)

            return wrapped

        return decorator

    def profile_data():
        path = ROOT / "profiles.json"
        return json.loads(path.read_text()) if path.exists() else {}

    def admin_users():
        return profile_data().get("admin_users", [])

    def rotation_required(username):
        return username in profile_data().get("rotation_required", [])

    @app.context_processor
    def context():
        from modules.resource_catalog import SERVICES, TABS
        from modules.resource_forms import FORMS

        return {
            "csrf": g.record.get("csrf", ""),
            "current_username": g.record.get("username", ""),
            "is_admin": g.record.get("username") in admin_users(),
            "app_version": "0.1.0",
            "current_profile_name": "local-admin-profile",
            "active_menu": request.endpoint,
            "page_title": {
                "audit_partitions": "Audit partitions",
                "dashboard": "Dashboard",
                "configuration": "Configuration & setup",
                "control": "Control Center",
                "messages": "Message Explorer",
                "audit": "Audit Center",
                "validation": "Source readiness",
                "password": "Change password",
            }.get(request.endpoint, "HeatWave Audit"),
            "services": SERVICES,
            "service_tabs": TABS,
            "resource_forms": FORMS,
            "queue_connections": [],
            "runtime_configuration": {},
            "app_title": "HeatWave Audit",
            "auth_shell": not bool(g.record.get("username")),
        }

    @app.route("/")
    def index():
        return redirect(url_for("dashboard" if g.record.get("username") else "login"))

    @app.route("/healthz")
    def health():
        return {"status": "alive"}

    @app.route("/notifications", methods=["GET"])
    @protected()
    def notifications():
        if g.record["username"] in admin_users():
            from modules.image_builds import inventory

            for job in reversed(inventory()):
                if job.get("actor") == g.record["username"] and job["status"] in {
                    "complete",
                    "failed",
                    "interrupted",
                }:
                    notify(
                        job["tag"] + ": " + job["message"],
                        "success" if job["status"] == "complete" else "error",
                        "build:" + job["id"] + ":" + job["status"],
                    )
        return jsonify(items=SESSIONS.notifications(g.record["connection_id"]))

    @app.route("/notifications/clear", methods=["POST"])
    @protected()
    def clear_notifications():
        SESSIONS.clear_notifications(g.record["connection_id"])
        return jsonify(items=[])

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            ip = request.remote_addr
            now = time.monotonic()
            with LOCK:
                recent = [t for t in ATTEMPTS.get(ip, []) if now - t < 300]
                if len(recent) >= 10:
                    abort(429, "Too many sign-in attempts. Try again in five minutes.")
                if len(ATTEMPTS) > 5000:
                    ATTEMPTS.clear()
                ATTEMPTS[ip] = recent + [now]
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")
            sid = None
            if username and password:
                sid = SESSIONS.create("local-admin-profile", username, password, local_profile())
                try:
                    db = CONNECTIONS.borrow(sid, local_profile(), username, password)
                    if username in admin_users():
                        repo.migrate_connections(db)
                        repo.migrate_resources(db)
                    repo.query(db, "SELECT id FROM flows LIMIT 1")
                except Exception:
                    CONNECTIONS.close(sid)
                    SESSIONS.clear(sid)
                    sid = None
            if sid:
                clear()
                session["sid"] = sid
                with LOCK:
                    ATTEMPTS.pop(ip, None)
                return redirect(url_for("dashboard"))
            flash(
                "Sign-in failed. Check your credentials and local repository availability.", "error"
            )
        return render_template("login.html")

    @app.route("/logout", methods=["POST"])
    @protected()
    def logout():
        clear()
        return redirect(url_for("login"))

    @app.route("/password", methods=["GET", "POST"])
    @protected()
    def password():
        if request.method == "POST":
            value = request.form.get("password", "")
            if len(value) < 14 or value != request.form.get("confirmation"):
                flash("Use matching passwords of at least 14 characters.", "error")
            else:
                repo.query(g.db, "ALTER USER CURRENT_USER() IDENTIFIED BY %s", (value,))
                with LOCK:
                    data = profile_data()
                    data["rotation_required"] = [
                        u for u in data.get("rotation_required", []) if u != g.record["username"]
                    ]
                    write_private_json(ROOT / "profiles.json", data)
                for sid, record in list(SESSIONS._records.items()):
                    if record.get("username") == g.record["username"]:
                        CONNECTIONS.close(sid)
                        SESSIONS.clear(sid)
                session.clear()
                return redirect(url_for("login"))
        return render_template("password.html")

    @app.route("/dashboard")
    @protected()
    def dashboard():
        flows = repo.flows(g.db)
        operations = repo.query(g.db, "SELECT * FROM operations ORDER BY id DESC LIMIT 8")
        return render_template(
            "dashboard.html",
            flows=flows,
            operations=operations,
            connection_count=repo.query(g.db, "SELECT COUNT(*) AS n FROM connections", one=True)[
                "n"
            ],
        )

    @app.route("/connections/<int:connection_id>/objects")
    @protected(admin=True)
    def connection_objects(connection_id):
        from modules.connections import connection_failure
        from modules.database_objects import discover

        row = repo.query(g.db, "SELECT * FROM connections WHERE id=%s", (connection_id,), one=True)
        if not row:
            abort(404)
        try:
            return jsonify(discover(row, Cloud(), request.args.get("schema")))
        except Exception as error:
            code, message = connection_failure(error)
            return jsonify(ok=False, code=code, message=message), 400

    @app.route("/connections/test", methods=["POST"])
    @protected(admin=True)
    def test_connection():
        from modules.connections import connection_failure, connection_values, verify_connection

        try:
            result = verify_connection(connection_values(request.form), Cloud())
        except Exception as error:
            code, message = connection_failure(error)
            return jsonify(ok=False, code=code, message=message), 400
        return jsonify(
            ok=True,
            message="Connected to MySQL " + result["server_version"] + " using TLS encryption.",
        )

    @app.route("/configuration/scope")
    @protected(admin=True)
    def resource_scope():
        cloud = Cloud()
        return jsonify(
            compartments=[{"id": cloud.compartment, "name": "VM compartment"}],
            selected=cloud.compartment,
            region=cloud.config["region"],
        )

    @app.route("/configuration/services/<kind>")
    @protected(admin=True)
    def service_inventory(kind):
        from modules.resource_catalog import inventory

        return jsonify(resources=inventory(Cloud(), kind, g.db))

    @app.route("/configuration/services/heatwave/actions/<action>", methods=["POST"])
    @protected(admin=True)
    def heatwave_lifecycle(action):
        from modules.heatwave_resources import lifecycle

        ids = [value for value in request.form.get("ids", "").split(",") if value]
        results = lifecycle(g.db, Cloud(), ids, action, g.record["username"])
        for result in results:
            if result["status"] == "submitted":
                flash(
                    result["name"] + ": " + action + " submitted. Refresh to check completion.",
                    "success",
                )
            else:
                flash(
                    result["name"] + ": " + result["message"] + " Request: " + result["request_id"],
                    "error",
                )
        return redirect(url_for("configuration", tab="resources", service="heatwave"))

    @app.route("/configuration/options/<kind>")
    @protected(admin=True)
    def service_options(kind):
        from modules.service_options import choices

        return jsonify(
            options=choices(
                Cloud(),
                kind,
                request.args.get("vault_id"),
                request.args.get("repository_id"),
                request.args.get("shape_name"),
                request.args.get("availability_domain"),
            )
        )

    @app.route("/configuration/services/<kind>/create", methods=["POST"])
    @protected(admin=True)
    def create_service(kind):
        cloud = Cloud()
        if kind == "container":
            if request.form.get("compartment_id") != cloud.compartment:
                raise ValueError("Select the VM compartment.")
            from modules.workflows import WorkflowService

            WorkflowService(g.db, lambda: cloud, g.record["username"]).action(
                int(request.form["flow_id"]), "deploy"
            )
        else:
            from modules.resource_catalog import create

            create(g.db, cloud, kind, request.form, g.record["username"])
        flash("OCI creation submitted. Refresh the service list for lifecycle status.", "success")
        return redirect(url_for("configuration", tab="resources", service=kind))

    @app.route("/configuration/services/<kind>/delete", methods=["POST"])
    @protected(admin=True)
    def delete_services(kind):
        from modules.resource_catalog import delete_selected

        ids = sorted(set(filter(None, request.form.get("ids", "").split(","))))
        delete_selected(g.db, Cloud(), kind, ids, g.record["username"])
        flash(
            "OCI deletion submitted. Referenced resources and compartment scope are checked before deletion.",
            "success",
        )
        return redirect(url_for("configuration", tab="resources", service=kind))

    @app.route("/queue-connections", methods=["POST"])
    @protected(admin=True)
    def add_queue_connection():
        from modules.configuration_entries import queue_values
        from modules.queue_connections import profile

        data = queue_values(request.form)
        profile(Cloud(), data)
        repo.query(
            g.db,
            "INSERT INTO queue_connections(name,kind,resource_ocid,username,secret_ocid) VALUES (%s,%s,%s,%s,%s)",
            list(data.values()),
        )
        return redirect(url_for("configuration", tab="resources", service="queues"))

    @app.route("/queue-connections/delete", methods=["POST"])
    @protected(admin=True)
    def delete_queue_connections():
        ids = [int(value) for value in request.form.get("ids", "").split(",") if value]
        if not 1 <= len(ids) <= 25:
            raise ValueError("Select between 1 and 25 queue connections.")
        with repo.named_lock(g.db, "mappings", "all"):
            for value in ids:
                if repo.query(
                    g.db,
                    "SELECT id FROM flows WHERE queue_connection_id=%s LIMIT 1",
                    (value,),
                    one=True,
                ):
                    raise ValueError("A selected queue connection is used by a mapping.")
            for value in ids:
                repo.query(g.db, "DELETE FROM queue_connections WHERE id=%s", (value,))
        return redirect(url_for("configuration", tab="resources", service="queues"))

    @app.route("/queue-connections/<int:queue_id>/topics")
    @protected(admin=True)
    def queue_topics(queue_id):
        from modules.queue_connections import topics

        row = repo.query(g.db, "SELECT * FROM queue_connections WHERE id=%s", (queue_id,), one=True)
        if not row:
            abort(404)
        return jsonify(topics=topics(Cloud(), row))

    @app.route("/queue-topics/create", methods=["POST"])
    @protected(admin=True)
    def create_queue_topic():
        from modules.queue_connections import create_topic

        with repo.named_lock(g.db, "mappings", "all"):
            row = repo.query(
                g.db,
                "SELECT * FROM queue_connections WHERE id=%s",
                (request.form.get("queue_connection_id"),),
                one=True,
            )
            if not row:
                raise ValueError("Choose a queue connection.")
            create_topic(
                g.db,
                Cloud(),
                row,
                request.form.get("name", ""),
                bounded(request.form.get("partitions"), 1, 1000),
            )
        flash(
            "Topic request completed. Matching streams already submitted by this app are reused. "
            "Refresh the topic list until its state is ACTIVE before selecting it in a mapping.",
            "success",
        )
        return redirect(
            url_for("configuration", tab="resources", service="queues", queue_id=row["id"])
        )

    @app.route("/queue-topics/delete", methods=["POST"])
    @protected(admin=True)
    def delete_queue_topics():
        from modules.queue_connections import delete_topics

        row = repo.query(
            g.db,
            "SELECT * FROM queue_connections WHERE id=%s",
            (request.args.get("queue_id"),),
            one=True,
        )
        if not row:
            raise ValueError("Choose a queue connection.")
        names = sorted(set(filter(None, request.form.get("ids", "").split(","))))
        delete_topics(g.db, Cloud(), row, names, g.record["username"])
        return redirect(url_for("configuration", tab="resources", service="queues"))

    @app.route("/flows/<int:flow_id>/prepare-topics", methods=["POST"])
    @protected(admin=True)
    def prepare_flow_topics(flow_id):
        from modules.queue_connections import prepare_schema_topic

        with repo.named_lock(g.db, "flow", flow_id):
            flow = repo.get_flow(g.db, flow_id)
            message = prepare_schema_topic(g.db, Cloud(), flow, g.record["username"])
        flash(message, "success")
        if request.args.get("return_to") == "control":
            return redirect(url_for("control"))
        return redirect(url_for("configuration", tab="flows"))

    @app.route("/configuration/image-builds")
    @protected(admin=True)
    def image_build_inventory():
        from modules import image_builds

        rows = image_builds.inventory()
        for row in rows:
            row["log_url"] = url_for("image_build_log", job_id=row["id"])
        return jsonify(builds=rows, worker_available=image_builds.worker_available())

    @app.route("/configuration/image-builds", methods=["POST"])
    @protected(admin=True)
    def create_image_build():
        from modules import image_builds

        if not image_builds.worker_available():
            raise ValueError(
                "The VM image builder is unavailable. Start heatwave-audit-image-builder.service first."
            )
        with repo.named_lock(g.db, "image-builds", "queue"):
            image_builds.enqueue(
                Cloud(),
                request.form.get("role", ""),
                request.form.get("repository_id", ""),
                request.form.get("compartment_id", ""),
                g.record["username"],
            )
        flash("Image build queued. Follow its progress in Container Registry.", "success")
        return redirect(url_for("configuration", tab="resources", service="repository"))

    @app.route("/configuration/image-builds/<job_id>/log")
    @protected(admin=True)
    def image_build_log(job_id):
        from modules import image_builds

        path = image_builds.job_path(job_id)
        job = image_builds.load(path)
        log = path / "build.log"
        if not log.is_file():
            return Response("The build has not produced a log yet.\n", mimetype="text/plain")
        return send_file(
            log, mimetype="text/plain", as_attachment=True, download_name=job["tag"] + ".log"
        )

    @app.route("/configuration/workloads/<role>", methods=["POST"])
    @protected(admin=True)
    def save_workload(role):
        if role not in {"debezium", "processor"}:
            abort(404)
        cloud = Cloud()
        from modules.config import save_config

        subnet = cloud.check(cloud.network.get_subnet(request.form.get("subnet_id", "")).data)
        cloud.check(cloud.network.get_vcn(subnet.vcn_id).data)
        repository = cloud.check(
            cloud.registry.get_container_repository(request.form.get("repository_id", "")).data
        )
        if repository.is_public:
            raise ValueError("Select a private registry repository.")
        image = cloud.check(
            cloud.registry.get_container_image(request.form.get("image_id", "")).data
        )
        if image.repository_id != repository.id:
            raise ValueError("Image does not belong to the selected repository.")
        from modules.image_registry import endpoint, pull_mode

        mode = pull_mode(
            {"image_pull_auth": request.form.get("image_pull_auth", "resource_principal")}
        )
        secret_id = request.form.get("image_pull_secret_ocid", "") if mode == "vault" else ""
        if mode == "vault":
            if not secret_id:
                raise ValueError("Select an image pull secret for Vault authentication.")
            cloud.check(cloud.vault.get_secret(secret_id).data)
        from modules.service_options import choices

        domain = request.form.get("availability_domain", "")
        shape = request.form.get("shape", "")
        if domain not in {r["id"] for r in choices(cloud, "availability_domain")}:
            raise ValueError("Select an availability domain in this region.")
        if shape not in {r["id"] for r in choices(cloud, "container_shape")}:
            raise ValueError("Select a supported OCI Container Instances shape.")
        current = cloud.config.get(role, {})
        registry_endpoint = endpoint(cloud.config["region"])
        from modules.resource_catalog import integer

        updated = {
            **current,
            "subnet_id": subnet.id,
            "repository_id": repository.id,
            "image_pull_secret_ocid": secret_id,
            "registry_endpoint": registry_endpoint,
            "image_pull_auth": mode,
            "image_release": image.version or image.display_name,
            "image_id": image.id,
            "image_url": registry_endpoint
            + "/"
            + repository.namespace
            + "/"
            + repository.display_name
            + "@"
            + image.digest,
            "availability_domain": domain,
            "shape": shape,
            "ocpus": integer(request.form, "ocpus", 1, 1, 64),
            "memory_in_gbs": integer(request.form, "memory_in_gbs", 4, 1, 512),
        }
        if role == "debezium":
            updated.setdefault("server_id_base", 54000)
        config = {**cloud.config, role: updated}
        save_config(config, cloud.compartment, config["region"])
        from modules.fingerprints import digest

        repo.query(
            g.db,
            "INSERT INTO config_revisions(actor,config_digest,config_document) VALUES (%s,%s,%s)",
            (g.record["username"], digest(config), json.dumps(config)),
        )
        flash(
            "OCI Container Instances workload settings saved. Existing instances keep their deployed configuration.",
            "success",
        )
        return redirect(url_for("configuration", tab="resources", service="container"))

    @app.route("/configuration", methods=["GET", "POST"])
    @protected(admin=True)
    def configuration():
        if request.method == "POST":
            action = request.form.get("action")
            if action == "connection":
                from modules.connections import connection_values, verify_connection

                with repo.named_lock(g.db, "mappings", "all"):
                    values = connection_values(request.form)
                    result = verify_connection(values, Cloud())
                    from modules.configuration_entries import connection_identity

                    values["server_identity"] = connection_identity(g.db, values, result)
                    repo.query(
                        g.db,
                        "INSERT INTO connections ("
                        + ",".join(values)
                        + ") VALUES ("
                        + ",".join(["%s"] * len(values))
                        + ")",
                        list(values.values()),
                    )
                flash("Connection registered. Password stays in OCI Vault.", "success")
            elif action == "flow":
                from modules.configuration_entries import mapping_values

                with repo.named_lock(g.db, "mappings", "all"):
                    data = mapping_values(g.db, Cloud(), request.form)
                    data["connector_name"] = "audit-" + data["name"] + "-" + secrets.token_hex(6)
                    repo.query(
                        g.db,
                        "INSERT INTO flows ("
                        + ",".join(data)
                        + ") VALUES ("
                        + ",".join(["%s"] * len(data))
                        + ")",
                        list(data.values()),
                    )
                flash(
                    "Flow saved as draft. Validate the source and choose Prepare schema topic "
                    "in Control Center before deployment.",
                    "success",
                )
            else:
                raise ValueError("Unknown configuration action.")
            return redirect(
                url_for(
                    "configuration",
                    tab={
                        "connection": "connections",
                        "flow": "flows",
                        "provision": "resources",
                    }.get(action, "connections"),
                )
            )
        edit = None
        if request.args.get("edit"):
            from modules.configuration_entries import edit_details

            edit = edit_details(
                g.db, request.args["edit"], bounded(request.args.get("entry_id"), 1, 2**63 - 1)
            )
            edit["url"] = url_for(
                "edit_configuration_entry", kind=edit["kind"], entry_id=edit["id"]
            )
        config = load_config()
        return render_template(
            "configuration.html",
            edit_entry=edit,
            connections=repo.query(g.db, "SELECT * FROM connections ORDER BY role,name"),
            flows=repo.flows(g.db),
            resources=config.get("resources", {}),
            provisioned=repo.query(
                g.db, "SELECT * FROM provisioned_resources ORDER BY created_at DESC"
            ),
            runtime_configuration=config,
            queue_connections=repo.query(g.db, "SELECT * FROM queue_connections ORDER BY name"),
            configured=bool(config),
            setup_sql=(ROOT / "sql/repository.sql").read_text(),
        )

    @app.route("/configuration/edit/<kind>/<int:entry_id>", methods=["POST"])
    @protected(admin=True)
    def edit_configuration_entry(kind, entry_id):
        from modules.configuration_entries import edit_entry

        changed = edit_entry(g.db, Cloud, kind, entry_id, request.form, g.record["username"])
        flash("Changes saved." if changed else "No changes to save.", "success")
        return redirect(
            url_for(
                "configuration",
                tab={"connection": "connections", "flow": "flows", "queue": "resources"}[kind],
                **({"service": "queues"} if kind == "queue" else {}),
            )
        )

    @app.route("/configuration/fingerprint/setup", methods=["POST"])
    @protected(admin=True)
    def setup_fingerprint_key():
        from modules.fingerprint_setup import setup
        from modules.fingerprints import digest

        result = setup(Cloud(), request.form.get("vault_id", ""), request.form.get("key_id", ""))
        config = load_config(required=True)
        repo.query(
            g.db,
            "INSERT INTO config_revisions(actor,config_digest,config_document) VALUES (%s,%s,%s)",
            (g.record["username"], digest(config), json.dumps(config)),
        )
        repo.record(g.db, None, "configure-fingerprint", "complete", g.record["username"])
        flash(
            "Row-fingerprint key configured in OCI Vault: "
            + result["name"]
            + ". The key value stays in Vault.",
            "success",
        )
        return redirect(url_for("runtime_settings"))

    @app.route("/configuration/runtime", methods=["GET", "POST"])
    @protected(admin=True)
    def runtime_settings():
        cloud = Cloud()
        if request.method == "POST":
            import hashlib

            from modules.config import save_config

            try:
                config = json.loads(request.form.get("configuration", ""))
            except json.JSONDecodeError:
                raise ValueError("Enter valid deployment JSON.")
            save_config(config, cloud.compartment, cloud.config["region"])
            digest = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
            repo.query(
                g.db,
                "INSERT INTO config_revisions(actor,config_digest,config_document) VALUES (%s,%s,%s)",
                (g.record["username"], digest, json.dumps(config)),
            )
            flash(
                "Runtime configuration saved. Existing containers keep their current effective revision.",
                "success",
            )
            return redirect(url_for("runtime_settings"))
        checks = {}
        if request.args.get("check") == "1":
            calls = {
                "Managed Kafka": lambda: cloud.kafka.list_kafka_clusters(
                    compartment_id=cloud.compartment
                ),
                "OCI Streaming": lambda: cloud.streams.list_stream_pools(cloud.compartment),
                "Container Instances": lambda: cloud.containers.list_container_instances(
                    cloud.compartment
                ),
                "Container Registry": lambda: cloud.registry.list_container_repositories(
                    cloud.compartment
                ),
                "HeatWave": lambda: cloud.mysql.list_db_systems(cloud.compartment),
                "Vault secrets": lambda: cloud.vault.list_secrets(cloud.compartment),
            }
            for name, call in calls.items():
                try:
                    call()
                    checks[name] = "Read access verified in VM compartment"
                except Exception:
                    checks[name] = (
                        "Access denied or unavailable; provision remains blocked until verified"
                    )
        config = load_config(required=True)
        example = (
            json.loads((ROOT / "examples/deployment.example.json").read_text())
            if (ROOT / "examples/deployment.example.json").exists()
            else {}
        )
        for key in (
            "kafka",
            "streaming",
            "processor",
            "debezium",
            "fingerprint_secret_ocid",
            "retention",
            "mysql_tls",
        ):
            config.setdefault(key, example.get(key, {}))
        return render_template("runtime_settings.html", configuration=config, checks=checks)

    @app.route("/flows/<int:flow_id>/validate", methods=["GET", "POST"])
    @protected(admin=True)
    def validation(flow_id):
        flow = repo.get_flow(g.db, flow_id)
        result = None
        if request.method == "POST":
            result = validate_source(
                flow["source"], flow["source_schema"], flow["source_table"], Cloud()
            )
            repo.record(
                g.db,
                flow_id,
                "validate",
                "passed" if result["ok"] else "blocked",
                g.record["username"],
            )
        return render_template("validation.html", flow=flow, result=result)

    @app.route("/control")
    @protected()
    def control():
        flows = repo.flows(g.db)
        return render_template("control.html", flows=flows)

    from modules.capture_pages import register as register_capture_pages

    register_capture_pages(app, protected, lambda: Cloud(), flash)

    @app.route("/flows/<int:flow_id>/status")
    @protected()
    def flow_status(flow_id):
        from modules.flow_status import snapshot

        flow = repo.query(
            g.db,
            "SELECT id,status,last_action,connector_ocid,processor_ocid FROM flows WHERE id=%s",
            (flow_id,),
            one=True,
        )
        if not flow:
            abort(404)
        return jsonify(snapshot(flow, Cloud))

    @app.route("/flows/<int:flow_id>/<action>", methods=["POST"])
    @protected(admin=True)
    def flow_action(flow_id, action):
        if action not in {
            "deploy",
            "start",
            "stop",
            "stop_all",
            "resume",
            "restart",
            "retire",
            "delete",
            "refresh",
            "update_debezium",
            "update_writer",
        }:
            abort(404)
        from modules.workflows import SourceNotReady, WorkflowService

        try:
            status = WorkflowService(g.db, Cloud, g.record["username"]).action(flow_id, action)
        except SourceNotReady as error:
            return render_template("validation.html", flow=error.flow, result=error.result), 409
        message = (
            "Flow status: " + status + "."
            if action == "refresh"
            else "Action submitted. Workload status refreshes automatically in Control Center."
        )
        if action in {"update_debezium", "update_writer"}:
            label = "Debezium" if action == "update_debezium" else "History writer"
            message = (
                f"{label} is still stopping. Wait for INACTIVE, then click its Update image action again to complete replacement. The other workload is unchanged."
                if status == "stopping"
                else f"{label} replacement submitted with the selected image. Its checkpoints are retained; the other workload is unchanged."
            )
        elif action == "stop":
            message = "Debezium stop requested. The history writer remains running to drain queued records and waits when caught up. Start resumes capture."
        elif action == "stop_all":
            message = "Stop requested for both workloads. Queued messages remain available within the messaging retention period."
        flash(message, "warning" if status in {"attention", "stopping"} else "success")
        return redirect(url_for("control"))

    @app.route("/connections/delete-selected", methods=["POST"])
    @protected(admin=True)
    def delete_connections():
        try:
            ids = sorted({int(v) for v in request.form.get("ids", "").split(",") if v})
        except ValueError:
            raise ValueError("Invalid connection selection.")
        if not ids or len(ids) > 250:
            raise ValueError("Select between 1 and 250 connections.")
        placeholders = ",".join(["%s"] * len(ids))
        with repo.named_lock(g.db, "mappings", "all"):
            used = repo.query(
                g.db,
                "SELECT id FROM flows WHERE source_id IN ("
                + placeholders
                + ") OR target_id IN ("
                + placeholders
                + ") LIMIT 1",
                ids + ids,
                one=True,
            )
            if used:
                raise ValueError(
                    "A selected connection is still used by a flow. Nothing was deleted."
                )
            repo.query(g.db, "DELETE FROM connections WHERE id IN (" + placeholders + ")", ids)
        flash("Selected unused registrations deleted. OCI resources are retained.", "success")
        return redirect(url_for("configuration"))

    @app.route("/flows/<int:flow_id>/terminate", methods=["POST"])
    @protected(admin=True)
    def terminate_flow(flow_id):
        from modules.workflows import WorkflowService

        WorkflowService(g.db, Cloud, g.record["username"]).action(flow_id, "terminate")
        flash("Termination submitted. Refresh after OCI finishes.", "success")
        return redirect(url_for("control"))

    @app.route("/messages")
    @protected()
    def messages():
        flows = repo.flows(g.db)
        records = []
        flow_id = request.args.get("flow_id")
        if flow_id:
            records = explore(repo.get_flow(g.db, bounded(flow_id)), Cloud())
        return render_template("messages.html", flows=flows, records=records)

    @app.route("/flows/<int:flow_id>/partitions", methods=["GET", "POST"])
    @protected(admin=True)
    def audit_partitions(flow_id):
        from modules.partitions import list_partitions, prepare
        from modules.retention import cleanup, policy_for, preview

        with repo.named_lock(g.db, "flow", flow_id):
            flow = repo.get_flow(g.db, flow_id)
            cloud = Cloud()
            policy_error = None
            try:
                policy = policy_for(cloud.config, flow_id)
            except ValueError as error:
                if request.method == "POST" and request.form.get("action") == "cleanup":
                    raise
                policy = None
                policy_error = str(error)
            jobs = []
            retention_plan = None
            with connect(flow["target"], cloud) as db:
                if request.method == "POST":
                    action = request.form.get("action")
                    if action == "prepare":
                        added = prepare(db, flow["target_schema"], flow["target_table"])
                        repo.record(
                            g.db, flow_id, "prepare-partitions", "complete", g.record["username"]
                        )
                        flash(
                            "Partition preparation completed: " + str(len(added)) + " added.",
                            "success",
                        )
                    elif action == "cleanup":
                        if not policy:
                            raise ValueError("Enable an explicit retention policy before cleanup.")
                        # Initial release requires a stopped flow. This also fences legacy
                        # images before adopting cleanup; live fleet cleanup needs a controller.
                        for field in ("processor_ocid", "connector_ocid"):
                            if flow[field]:
                                instance = cloud.owned_container(flow[field], flow_id)
                                if instance.lifecycle_state != "INACTIVE":
                                    raise ValueError(
                                        "Use Control Center → More actions → Stop both workloads and wait for INACTIVE before cleaning history."
                                    )
                        if flow["processor_ocid"]:
                            cloud.require_writer_protocol(flow["processor_ocid"], flow_id)
                        try:
                            job_id = cleanup(db, flow, policy, g.record["username"])
                        except Exception:
                            repo.record(g.db, flow_id, "cleanup", "failed", g.record["username"])
                            raise
                        repo.record(g.db, flow_id, "cleanup", "complete", g.record["username"])
                        flash(
                            "Cleanup completed. Job: " + job_id
                            if job_id
                            else "No eligible partitions.",
                            "success",
                        )
                    else:
                        raise ValueError("Unknown partition action.")
                partitions = list_partitions(db, flow["target_schema"], flow["target_table"])
                if policy:
                    retention_plan = preview(db, flow, policy)
                    from modules.validation import quote

                    jobs = repo.query(
                        db,
                        f"SELECT job_id,cutoff,status,created_at FROM "
                        f"{quote(flow['target_schema'])}.audit_cleanup_jobs WHERE flow_id=%s "
                        "ORDER BY created_at DESC LIMIT 20",
                        (flow_id,),
                    )
        return render_template(
            "partitions.html",
            flow=flow,
            partitions=partitions,
            policy=policy,
            policy_error=policy_error,
            retention_plan=retention_plan,
            jobs=jobs,
        )

    @app.route("/audit")
    @protected()
    def audit():
        flows = repo.flows(g.db)
        if request.args.get("tab") == "audit-global" or (
            not request.args.get("tab") and not request.args.get("flow_id")
        ):
            chosen = [
                f
                for f in flows
                if (
                    not request.args.get("source_id")
                    or str(f["source_id"]) == request.args["source_id"]
                )
                and (
                    not request.args.get("source_schema")
                    or f["source_schema"] == request.args["source_schema"]
                )
            ]
            if len(chosen) > 100:
                raise ValueError(
                    "Global charts support 100 tables per view. Select a source database to narrow the view."
                )
            summary = overview(
                [repo.get_flow(g.db, f["id"]) for f in chosen],
                Cloud() if chosen else None,
                request.args.to_dict(),
            )
            return render_template("audit_global.html", flows=flows, overview=summary)
        result = None
        flow = None
        if request.args.get("flow_id"):
            flow = repo.get_flow(g.db, bounded(request.args["flow_id"]))
            result = browse(flow, Cloud(), request.args)
            if request.args.get("export") == "csv":
                out = io.StringIO()
                writer = csv.writer(out)
                columns = [
                    "event_id",
                    "source_schema",
                    "source_table",
                    "operation",
                    "event_time",
                    "record_key",
                    "before_image",
                    "after_image",
                ]
                writer.writerow(columns)
                for row in result["rows"]:
                    cells = []
                    for column in columns:
                        value = row[column]
                        value = (
                            json.dumps(value, default=str)
                            if isinstance(value, (dict, list))
                            else str(value or "")
                        )
                        cells.append(
                            "'" + value
                            if value.startswith(("=", "+", "-", "@", "\t", "\r"))
                            else value
                        )
                    writer.writerow(cells)
                return Response(
                    out.getvalue(),
                    mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=audit-page.csv"},
                )
        args = request.args.to_dict()
        args.pop("export", None)
        return render_template(
            "audit.html",
            flows=flows,
            result=result,
            flow=flow,
            export_args={**args, "export": "csv"},
            prev_args={**args, "page": max(1, (result or {}).get("page", 1) - 1)},
            next_args={**args, "page": (result or {}).get("page", 1) + 1},
        )

    @app.errorhandler(ValueError)
    def value_error(error):
        notify(str(error), "error")
        if request.accept_mimetypes.best == "application/json":
            return jsonify(ok=False, message=str(error)), 400
        return render_template("error.html", message=str(error)), 400

    @app.errorhandler(Exception)
    def failure(error):
        from oci.exceptions import ServiceError
        from werkzeug.exceptions import HTTPException

        from modules.mysql_util import MySQLError

        if isinstance(error, HTTPException):
            return render_template("error.html", message=error.description), error.code
        if isinstance(error, ServiceError):
            from modules.service_errors import oci_failure

            details = oci_failure(error)
            notify(
                details["message"]
                + " "
                + details["guidance"]
                + " Request: "
                + details["request_id"],
                "error",
            )
            status = details.pop("response_status")
            app.logger.warning(
                "OCI request failed: %s code=%s request_id=%s",
                details["operation"],
                details["code"],
                details["request_id"],
            )
            if request.accept_mimetypes.best == "application/json":
                return jsonify(
                    ok=False,
                    **{**details, "message": details["message"] + " " + details["guidance"]},
                ), status
            return render_template(
                "error.html", **details, return_url=url_for("configuration", tab="resources")
            ), status
        if isinstance(error, MySQLError):
            from modules.service_errors import mysql_failure

            details = mysql_failure(error, request.endpoint)
            status = details.pop("response_status")
            message = details["message"] + " " + details["guidance"]
            app.logger.warning(
                "MySQL request failed: endpoint=%s code=%s type=%s",
                request.endpoint,
                details["code"],
                type(error).__name__,
            )
            notify(message, "error")
            if request.accept_mimetypes.best == "application/json":
                return jsonify(ok=False, message=message, code=details["code"]), status
            return render_template(
                "error.html",
                **details,
                return_url=url_for("control"),
                return_label="Return to Control Center",
            ), status
        # Never expose SQL, passwords, OCI response bodies, connection details or row values.
        app.logger.warning(
            "Operation failed: endpoint=%s type=%s", request.endpoint, type(error).__name__
        )
        notify(
            "Operation could not complete. Check service access, private settings and resource readiness.",
            "error",
        )
        if request.accept_mimetypes.best == "application/json":
            return jsonify(
                ok=False,
                message="Service access denied or unavailable in the VM compartment. Check the VM identity permissions and resource readiness.",
            ), 503
        return render_template(
            "error.html",
            message="Operation could not complete. Check service access, private settings and resource readiness.",
        ), 503

    return app


app = create_app()
if __name__ == "__main__":
    app.run(
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "5000")),
        threaded=True,
    )
