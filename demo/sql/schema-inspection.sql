-- Read-only schema inspection on the target. Replace flow_id with the mapping ID.
-- object_name follows the identifier supplied by the schema event; inspect it first.
SELECT flow_id, object_name, change_kind, fidelity, observed_at
FROM demo_audit.audit_schema_changes
WHERE flow_id = 1
ORDER BY observed_at;

SELECT change_kind, before_definition, after_definition, semantic_delta, fidelity
FROM demo_audit.audit_schema_changes
WHERE flow_id = 1 AND object_name = 'demo_sales.orders'
ORDER BY observed_at;

-- Interpret only captured metadata. Missing index/partition/default properties
-- are unknown, not proof that those structures or defaults do not exist.
