"""Prepare explicitly selected targets on the OL9 VM; never reset flow identity or data."""

import argparse
import getpass
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.config import ROOT, local_profile  # noqa: E402
from modules.heatwave import connect, prepare_target  # noqa: E402
from modules.mysql_util import open_connection  # noqa: E402
from modules.oci_service import Cloud  # noqa: E402
from modules.repository import get_flow, named_lock  # noqa: E402
from modules.runtime_env import load_runtime_defaults  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flow-id", type=int, action="append", required=True)
    parser.add_argument("--username", default="localadmin")
    args = parser.parse_args()
    load_runtime_defaults(ROOT / ".runtime.env", os.environ)
    if any(value <= 0 for value in args.flow_id):
        parser.error("Flow IDs must be positive.")
    password = getpass.getpass("Local repository password: ")
    db, _ = open_connection(local_profile(), args.username, password)
    try:
        cloud = Cloud()  # Instance principal and VM compartment validation only.
        for flow_id in sorted(set(args.flow_id)):
            with named_lock(db, "flow", flow_id):
                flow = get_flow(db, flow_id)
                for field in ("connector_ocid", "processor_ocid"):
                    if flow[field]:
                        resource = cloud.owned_container(flow[field], flow_id)
                        if resource.lifecycle_state not in {"INACTIVE", "DELETED"}:
                            raise ValueError("Stop the selected flow before preparing its target.")
                with connect(flow["target"], cloud) as target:
                    prepare_target(target, flow)
                print(
                    f"Target preparation complete for flow {flow_id}. History and identity retained."
                )
    finally:
        db.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(
            f"Target preparation stopped ({type(error).__name__}); review private configuration and table compatibility.",
            file=sys.stderr,
        )
        raise SystemExit(1)
