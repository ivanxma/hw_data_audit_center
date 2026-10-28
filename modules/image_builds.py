"""Fixed application builds, private job state and one OL9 build worker."""

import hashlib
import json
import re
import shutil
import uuid
from datetime import datetime, timezone

from .config import ROOT, write_private_json
from .image_registry import destination

ROLES = {"processor": "historywriter", "debezium": "debezium"}
ACTIVE = {"queued", "building", "checking", "publishing", "verifying"}


def folder():
    path = ROOT / ".data/image-builds"
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    return path


def job_path(job_id):
    if not re.fullmatch(r"[a-f0-9]{32}", job_id):
        raise ValueError("Invalid build selection.")
    path = folder() / job_id
    if path.is_symlink() or not path.is_dir():
        raise ValueError("Build not found.")
    return path


def save(path, value):
    write_private_json(path / "job.json", value)


def load(path):
    return json.loads((path / "job.json").read_text())


def inventory():
    rows = []
    for path in folder().glob("*/job.json"):
        if path.parent.is_symlink():
            continue
        row = json.loads(path.read_text())
        rows.append(
            {
                key: row.get(key, "")
                for key in (
                    "id",
                    "actor",
                    "role",
                    "tag",
                    "repository_name",
                    "status",
                    "created_at",
                    "revision",
                    "digest",
                    "message",
                    "image_url",
                    "image_id",
                )
            }
        )
    return sorted(rows, key=lambda row: row["created_at"], reverse=True)[:100]


def release_tag(role, revision, now=None):
    if role not in ROLES or not re.fullmatch(r"[a-f0-9]{64}", revision):
        raise ValueError("Invalid image release.")
    return (
        ROLES[role]
        + "-"
        + (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + revision[:12]
    )


def snapshot(source, context, role):
    if role not in ROLES:
        raise ValueError("Choose Debezium or the processor.")
    files = ["deploy/Containerfile." + role]
    files += [str(p.relative_to(source)) for p in (source / "modules").rglob("*.py")]
    if role == "processor":
        files += ["worker.py"]
    else:
        files += ["deploy/debezium_bootstrap.py"]
        files += [str(p.relative_to(source)) for p in (source / "deploy/native-capture").rglob("*")
                  if p.is_file() and "target" not in p.parts]
    manifest = {}
    context.mkdir(mode=0o700)
    for name in sorted(files):
        src = source / name
        if src.is_symlink() or source.resolve() not in src.resolve().parents or not src.is_file():
            raise ValueError("The application build context contains an invalid source file.")
        content = src.read_bytes()
        target = context / name
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        target.write_bytes(content)
        target.chmod(0o600)
        manifest[name] = hashlib.sha256(content).hexdigest()
    revision = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    return manifest, revision


def enqueue(cloud, role, repository_id, compartment, actor):
    if compartment != cloud.compartment:
        raise ValueError("Select the VM compartment.")
    if role not in ROLES:
        raise ValueError("Choose Debezium or the processor.")
    repository, prefix = destination(cloud, repository_id)
    if len([r for r in inventory() if r["status"] in ACTIVE]) >= 4:
        raise ValueError("Four builds are already queued or running. Wait for a build to finish.")
    job_id = uuid.uuid4().hex
    path = folder() / job_id
    path.mkdir(mode=0o700)
    try:
        manifest, revision = snapshot(ROOT, path / "context", role)
        tag = release_tag(role, revision)
        if any(
            r["tag"] == tag and r["repository_name"] == repository.display_name for r in inventory()
        ):
            raise ValueError("This release was just queued. Refresh the build list.")
        job = {
            "id": job_id,
            "role": role,
            "runtime_protocol": 3,
            "repository_id": repository.id,
            "repository_name": repository.display_name,
            "compartment_id": cloud.compartment,
            "region": cloud.config["region"],
            "tag": tag,
            "revision": revision,
            "image_url": prefix + ":" + tag,
            "status": "queued",
            "actor": actor,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "manifest": manifest,
            "message": "Waiting for the image build worker.",
        }
        save(path, job)
        return job_id
    except Exception:
        shutil.rmtree(path)
        raise


def worker_available():
    heartbeat = folder() / "heartbeat"
    try:
        return datetime.now().timestamp() - heartbeat.stat().st_mtime < 15
    except OSError:
        return False
