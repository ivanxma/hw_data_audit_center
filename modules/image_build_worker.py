"""Serial, fixed-context rootless Podman builds on the deployment VM."""

import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import threading
import time
from pathlib import Path

import oci

from . import image_builds as jobs
from .config import ROOT, load_config
from .image_registry import destination, write_push_auth
from .oci_service import Cloud


def runtime_directory():
    # systemd supplies an app-owned directory on /run, which is cleared at boot.
    return Path(
        os.environ.get("AUDIT_IMAGE_RUNTIME_DIR", str(ROOT / ".data/image-builder/runtime"))
    )


def podman(*args):
    base = ROOT / ".data/image-builder"
    base.mkdir(mode=0o700, exist_ok=True)
    runtime = runtime_directory()
    runtime.mkdir(mode=0o700, exist_ok=True)
    return [
        "/usr/bin/podman",
        "--root",
        # Keep older cache metadata intact for rollback when changing the runroot.
        str(base / "storage-v2"),
        "--runroot",
        str(runtime / "storage"),
        "--tmpdir",
        str(runtime / "libpod/tmp"),
        "--storage-driver=overlay",
        "--cgroup-manager=cgroupfs",
        "--events-backend=file",
        *args,
    ]


def run(path, *args, timeout=1800):
    environment = dict(os.environ)
    environment["XDG_RUNTIME_DIR"] = str(runtime_directory())
    # The builder does not pass deployment references or ambient proxy credentials to builds.
    for name in (
        "AUDIT_CONFIG_FILE",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ):
        environment.pop(name, None)
    command = podman(*args)
    with (path / "build.log").open("ab") as log:
        subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=environment,
            timeout=timeout,
            check=True,
        )


def images(cloud, repository_id):
    data = oci.pagination.list_call_get_all_results(
        cloud.registry.list_container_images,
        cloud.compartment,
        repository_id=repository_id,
    ).data
    rows = data if isinstance(data, list) else data.items
    return [cloud.check(row) for row in rows if row.lifecycle_state != "DELETED"]


def validate_context(path, job):
    context = path / "context"
    manifest = job["manifest"]
    revision = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    if revision != job["revision"] or job["role"] not in jobs.ROLES:
        raise ValueError("Build source revision does not match its manifest.")
    prefix = jobs.ROLES[job["role"]]
    if not re.fullmatch(re.escape(prefix) + r"-\d{8}T\d{6}Z-" + revision[:12], job["tag"]):
        raise ValueError("Invalid release tag.")
    actual = set()
    for file in context.rglob("*"):
        if file.is_symlink():
            raise ValueError("Build context must not contain symbolic links.")
        if file.is_file():
            name = file.relative_to(context).as_posix()
            actual.add(name)
            if hashlib.sha256(file.read_bytes()).hexdigest() != manifest.get(name):
                raise ValueError("Build context changed after it was queued.")
    if actual != set(manifest):
        raise ValueError("Build context is incomplete.")


def process(path):
    job = jobs.load(path)
    auth = path / "push-auth.json"
    local_image = "localhost/dataaudit-build:" + job["id"]

    def state(status, message):
        job.update(status=status, message=message)
        jobs.save(path, job)

    stage = "validating"
    try:
        cloud = Cloud()
        if cloud.compartment != job["compartment_id"] or cloud.config["region"] != job["region"]:
            raise ValueError("Build scope no longer matches the VM.")
        _, prefix = destination(cloud, job["repository_id"])
        if job["image_url"] != prefix + ":" + job["tag"]:
            raise ValueError("Registry destination changed after the build was queued.")
        validate_context(path, job)
        if any(r.version == job["tag"] for r in images(cloud, job["repository_id"])):
            raise ValueError("This release already exists. Queue a new release.")
        if shutil.disk_usage(ROOT).free < 3 * 1024**3:
            raise ValueError("At least 3 GiB of free VM disk space is required to build an image.")
        stage = "building"
        state(stage, "Building application source on the VM.")
        run(
            path,
            "build",
            "--layers=true",
            "--pull=always",
            "--format=oci",
            "--tag",
            local_image,
            "--file",
            str(path / "context/deploy" / ("Containerfile." + job["role"])),
            str(path / "context"),
        )
        stage = "checking"
        state(stage, "Checking the image runtime and application imports.")
        python = "python" if job["role"] == "processor" else "/opt/vault-python/bin/python"
        check = (
            "import worker; import mysql.connector; import confluent_kafka; import oci; "
            "from modules.principal_kafka import java_runtime; java_runtime()"
            if job["role"] == "processor"
            else "import sys, pathlib; sys.path.insert(0, '/opt'); import debezium_bootstrap; "
            "import modules.capture_runtime; import mysql.connector; "
            "assert pathlib.Path('/docker-entrypoint.sh').is_file()"
        )
        run(
            path,
            "run",
            "--rm",
            "--network=none",
            "--entrypoint",
            python,
            local_image,
            "-c",
            check,
            timeout=120,
        )
        run(
            path,
            "run",
            "--rm",
            "--network=none",
            "--entrypoint",
            "java",
            local_image,
            "-cp",
            "/opt/streaming-check:/opt/streaming-libs/*:/kafka/libs/*",
            "PrincipalRuntimeCheck",
            *(["connect"] if job["role"] == "debezium" else []),
            timeout=120,
        )
        stage = "publishing"
        state(stage, "Uploading the image to the selected OCI Registry repository.")
        # Recheck scope and repository immediately before a registry mutation.
        cloud = Cloud()
        if cloud.compartment != job["compartment_id"] or cloud.config["region"] != job["region"]:
            raise ValueError("VM scope changed during the build.")
        if any(r.version == job["tag"] for r in images(cloud, job["repository_id"])):
            raise ValueError("This release already exists. Queue a new release.")
        if write_push_auth(cloud, job["repository_id"], auth) != prefix:
            raise ValueError("Registry destination changed during the build.")
        try:
            run(
                path,
                "push",
                "--authfile",
                str(auth),
                "--digestfile",
                str(path / "digest"),
                "--retry=0",
                local_image,
                "docker://" + job["image_url"],
                timeout=900,
            )
        finally:
            auth.unlink(missing_ok=True)
        digest = (path / "digest").read_text().strip()
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise ValueError("Registry returned an invalid image digest.")
        stage = "verifying"
        state(stage, "Verifying the published release in OCI Registry.")
        for _ in range(30):
            matches = [r for r in images(cloud, job["repository_id"]) if r.version == job["tag"]]
            if matches:
                image = matches[0]
                if image.repository_id != job["repository_id"] or image.digest != digest:
                    raise ValueError(
                        "Published image does not match the selected repository and digest."
                    )
                job.update(digest=digest, image_id=image.id, image_url=prefix + "@" + digest)
                state(
                    "complete",
                    "Published. Select this release in OCI Container Instances workload settings.",
                )
                break
            time.sleep(2)
        else:
            raise ValueError(
                "The upload finished but OCI inventory has not confirmed it. Refresh image releases before retrying."
            )
    except Exception as error:
        # Do not persist raw SDK/HTTP exceptions, which may contain signed request details.
        message = (
            str(error)
            if isinstance(error, ValueError)
            else "Build failed during "
            + stage
            + ". Download the build log and check service access."
        )
        if stage in {"publishing", "verifying"}:
            message += " The release may exist in Registry; check it before queueing another build."
        state("failed", message)
    finally:
        auth.unlink(missing_ok=True)
        try:
            run(path, "image", "rm", "--no-prune", local_image, timeout=60)
        except (OSError, subprocess.SubprocessError):
            pass
        shutil.rmtree(path / "context", ignore_errors=True)


def main():
    os.umask(0o077)
    load_config(required=True)  # Do not advertise a ready worker without its VM configuration.
    if not Path("/usr/bin/podman").is_file():
        raise SystemExit("Install Podman on the OL9 VM first.")
    root = jobs.folder()
    with (root / "worker.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for row in jobs.inventory():
            path = jobs.job_path(row["id"])
            (path / "push-auth.json").unlink(missing_ok=True)
            if row["status"] in jobs.ACTIVE - {"queued"}:
                job = jobs.load(path)
                job.update(
                    status="interrupted",
                    message="Builder restarted. Check Registry for this release before starting another build.",
                )
                jobs.save(path, job)
                shutil.rmtree(path / "context", ignore_errors=True)
        stop = threading.Event()

        def heartbeat():
            while not stop.is_set():
                (root / "heartbeat").touch(mode=0o600)
                stop.wait(3)

        thread = threading.Thread(target=heartbeat, daemon=True)
        thread.start()
        try:
            while True:
                queued = [row for row in jobs.inventory() if row["status"] == "queued"]
                if queued:
                    process(jobs.job_path(queued[-1]["id"]))
                else:
                    time.sleep(2)
        finally:
            stop.set()
            (root / "heartbeat").unlink(missing_ok=True)
