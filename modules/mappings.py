"""Validate physical mappings across connection aliases, not merely registration IDs."""

from .repository import query

SYSTEM_SCHEMAS = {"mysql", "sys", "information_schema", "performance_schema"}
INTERNAL_TABLES = {
    "audit_event_dedup",
    "audit_schema_changes",
    "audit_retention_state",
    "audit_cleanup_jobs",
    "audit_delivery_checkpoints",
}


def validate_mapping(db, flow):
    if flow["source_schema"].lower() in SYSTEM_SCHEMAS:
        raise ValueError("System schemas cannot be registered for business-row capture.")
    source = (flow["source"]["server_identity"], flow["source_schema"], flow["source_table"])
    target = (flow["target"]["server_identity"], flow["target_schema"], flow["target_table"])
    if source == target:
        raise ValueError("Source and audit target must be different tables.")
    if flow["target_table"] in INTERNAL_TABLES or flow["target_table"].startswith(
        "debezium_history_"
    ):
        raise ValueError("Target name is reserved for audit recovery or control state.")
    rows = query(
        db,
        """SELECT f.id,f.source_schema,f.source_table,f.target_schema,f.target_table,
        s.server_identity AS source_system,t.server_identity AS target_system
        FROM flows f JOIN connections s ON s.id=f.source_id JOIN connections t ON t.id=f.target_id""",
    )
    # Include this proposed mapping when checking whether it captures its own state.
    target_namespaces = {(r["target_system"], r["target_schema"]) for r in rows}
    target_namespaces.add(target[:2])
    if source[:2] in target_namespaces and (
        source[2] in INTERNAL_TABLES or source[2].startswith("debezium_history_")
    ):
        raise ValueError("Audit control and recovery tables must not be captured.")
    for row in rows:
        other_source = (row["source_system"], row["source_schema"], row["source_table"])
        other_target = (row["target_system"], row["target_schema"], row["target_table"])
        if source == other_target or target == other_source:
            raise ValueError("Mapping would capture audit output and create a feedback loop.")
        if row["id"] == flow.get("id"):
            continue
        if source == other_source:
            raise ValueError(
                "This physical source table is already registered through another connection."
            )
        if target == other_target:
            raise ValueError("An audit target table must belong to exactly one flow.")
        if other_source[:2] == target[:2] and (
            other_source[2] in INTERNAL_TABLES or other_source[2].startswith("debezium_history_")
        ):
            raise ValueError(
                "The target namespace contains an existing capture of audit recovery state."
            )
