"""Validate physical mappings across connection aliases, not merely registration IDs."""

from .repository import query

from .capture_scope import SYSTEM_SCHEMAS, matches, overlaps

INTERNAL_TABLES = {
    "audit_event_dedup",
    "audit_schema_changes",
    "audit_retention_state",
    "audit_cleanup_jobs",
    "audit_delivery_checkpoints",
    "audit_capture_requests",
    "audit_initial_capture",
    "audit_native_offsets",
    "audit_message_assemblies",
    "audit_message_fragments",
    "audit_target_routes",
    "audit_target_route_claims",
}


def validate_mapping(db, flow):
    if flow["source_schema"].lower() in SYSTEM_SCHEMAS:
        raise ValueError("System schemas cannot be registered for business-row capture.")
    source = (flow["source"]["server_identity"], flow["source_schema"], flow["source_table"])
    target = (flow["target"]["server_identity"], flow["target_schema"], flow["target_table"])
    if source[0] == target[0] and matches(flow, *target[1:]):
        raise ValueError("Source and audit target must be different tables.")
    if flow["target_table"] in INTERNAL_TABLES or flow["target_table"].startswith(
        "debezium_history_"
    ):
        raise ValueError("Target name is reserved for audit recovery or control state.")
    rows = query(
        db,
        """SELECT f.id,f.source_schema,f.source_table,f.source_exclusions,f.target_grouping,f.target_schema,f.target_table,
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
    if flow['source_table'] == '*' and flow['source_schema'] != '*' and source[:2] in target_namespaces:
        raise ValueError('Database-wide capture cannot include an audit target schema on the same server.')
    if flow['source_schema'] == '*':
        from .capture_scope import exclusions
        if any(server == source[0] and namespace not in exclusions(flow) for server, namespace in target_namespaces):
            raise ValueError('All-database capture must exclude every audit target schema on this server.')
    for row in rows:
        other_source = (row["source_system"], row["source_schema"], row["source_table"])
        other_target = (row["target_system"], row["target_schema"], row["target_table"])
        if (row.get('target_grouping') or 'flow') != 'flow' and source[:2] == other_target[:2]:
            raise ValueError('Grouped audit target schemas cannot be captured as a source.')
        source_captures_target = source[0] == other_target[0] and matches(flow, *other_target[1:])
        other_captures_target = other_source[0] == target[0] and matches(row, *target[1:])
        if source_captures_target or other_captures_target:
            raise ValueError("Mapping would capture audit output and create a feedback loop.")
        if row["id"] == flow.get("id"):
            continue
        if source[0] == other_source[0] and overlaps(flow, row):
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
