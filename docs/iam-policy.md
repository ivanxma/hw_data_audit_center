# IAM and database access

Reviewed 24 October 2026 against the implemented OCI calls. An OCI administrator
applies these policies; the application and setup scripts do not create IAM,
change shared networking or switch to API-key credentials.

## Scope and identities

Use the VM compartment OCID for every resource policy below. The application
checks its configured compartment and region against instance metadata, then
checks each referenced resource's compartment. Subnets and their VCNs must also
belong to that compartment. Database connections use the supplied host and a real
MySQL login test; they do not prove a DB system's compartment from its IP address.

OCI compartment policies can also apply to descendants. The app's equality
checks remain stricter. For isolation outside the app as well, use a dedicated
leaf compartment or have the IAM administrator review service-specific conditions;
resource-variable conditions cannot be used as a generic list-result filter.
[Oracle policy variables](https://docs.oracle.com/en-us/iaas/Content/Identity/Reference/policyreference.htm).

Create two dynamic groups, with these matching-rule templates:

```text
ALL {instance.id = '<VM_OCID>', instance.compartment.id = '<VM_COMPARTMENT_OCID>'}
ALL {resource.type = 'computecontainerinstance', resource.compartment.id = '<VM_COMPARTMENT_OCID>'}
```

The first is the **VM group**; the second is the **container group**. The container
rule includes every Container Instance in that compartment, not only this app.
Use a dedicated compartment or administrator-managed membership restrictions
where workloads have different trust levels. App free-form ownership tags control
app actions; they are not an IAM isolation boundary.

In the statements below, `<VM_DYNAMIC_GROUP>` and `<CONTAINER_DYNAMIC_GROUP>` mean
the actual group subject, including its identity-domain prefix when applicable:
`'<IDENTITY_DOMAIN>'/'<GROUP_NAME>'`. Do not omit a non-default domain.
[Policy subjects](https://docs.oracle.com/en-us/iaas/Content/Identity/policysyntax/subject.htm),
[instance principals](https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm).

## VM: core deployment, image publishing and resource discovery

```text
Allow dynamic-group <VM_DYNAMIC_GROUP> to manage compute-container-family in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <VM_DYNAMIC_GROUP> to manage repos in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <VM_DYNAMIC_GROUP> to read vcns in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <VM_DYNAMIC_GROUP> to use subnets in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <VM_DYNAMIC_GROUP> to use vnics in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <VM_DYNAMIC_GROUP> to use network-security-groups in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <VM_DYNAMIC_GROUP> to read vaults in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <VM_DYNAMIC_GROUP> to read keys in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <VM_DYNAMIC_GROUP> to read secrets in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <VM_DYNAMIC_GROUP> to read secret-bundles in compartment id <VM_COMPARTMENT_OCID>
```

These support container lifecycle and log reads, private repository creation/push,
existing network attachments, secret dropdowns and password/key retrieval.
They do not grant network creation or security-rule administration. Availability
domain discovery uses the instance principal's existing compartment-inspect access.
[Container Instances policy reference](https://docs.oracle.com/en-us/iaas/Content/container-instances/permissions/policy-reference.htm),
[network permissions](https://docs.oracle.com/en-us/iaas/Content/Identity/Reference/corepolicyreference.htm).

## Optional VM provisioning capabilities

Enable only the services operated through the configuration screen.

```text
Allow dynamic-group <VM_DYNAMIC_GROUP> to manage stream-family in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <VM_DYNAMIC_GROUP> to manage mysql-family in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <VM_DYNAMIC_GROUP> to manage vaults in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <VM_DYNAMIC_GROUP> to manage secrets in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <VM_DYNAMIC_GROUP> to use keys in compartment id <VM_COMPARTMENT_OCID>
```

`stream-family` covers streams, stream pools and Connect configurations, including
messaging access. It replaces separate management statements for these Streaming
resources; it does **not** cover managed Apache Kafka. Existing private endpoints
still need the network attachment permissions above.
[Streaming policy reference](https://docs.oracle.com/en-us/iaas/Content/Identity/Reference/streamingpolicyreference.htm).

`mysql-family` supports DB system provisioning/start/stop and configuration reads.
A narrower deployment can use `manage mysql-instances`, `read mysql-configurations`
and `read mysql-work-requests` instead, with any additional backup/channel grants
required by the DB operation. Oracle-owned default configurations are service
catalogue choices; custom configurations must belong to the VM compartment.
[HeatWave resource types](https://docs.oracle.com/en-us/iaas/mysql-database/doc/resource-types.html),
[operation permissions](https://docs.oracle.com/en-us/iaas/mysql-database/doc/individual-resource-types.html).

Secret creation, including **Generate and configure key**, needs secret creation,
vault secret creation and key encrypt/decrypt permissions. Existing AES keys are
selected; this app does not create encryption keys. If vault creation/deletion
is disabled operationally, use `use vaults` instead of `manage vaults`.
[Vault and secret permissions](https://docs.oracle.com/en-us/iaas/Content/Identity/Reference/keypolicyreference.htm).

## Containers: image pulls, secrets and Streaming data plane

```text
Allow dynamic-group <CONTAINER_DYNAMIC_GROUP> to read repos in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <CONTAINER_DYNAMIC_GROUP> to read secrets in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <CONTAINER_DYNAMIC_GROUP> to read secret-bundles in compartment id <VM_COMPARTMENT_OCID>
Allow dynamic-group <CONTAINER_DYNAMIC_GROUP> to {STREAM_INSPECT, STREAM_READ, STREAM_CONSUME, STREAM_PRODUCE} in compartment id <VM_COMPARTMENT_OCID>
```

The containers have their own resource principals; they do not inherit the VM's
permissions. `read secrets` is necessary for the workload's compartment check;
`read secret-bundles` supplies the actual value. Restrict both statements to the
approved secret OCIDs with `target.secret.id` conditions when managing narrower
workload groups. The connector uses its source and target credentials; the writer
uses target and fingerprint secrets. Kafka credential/pull secrets are additional
only when their modes are configured. A container needs no provisioning grants.
[Private pulls](https://docs.oracle.com/en-us/iaas/Content/container-instances/permissions/policy-reference.htm),
[secret metadata and bundles](https://docs.oracle.com/en-us/iaas/Content/Identity/Reference/keypolicyreference.htm).

## Managed Apache Kafka alternative

For cluster/configuration lifecycle management, add:

```text
Allow dynamic-group <VM_DYNAMIC_GROUP> to {KAFKA_CLUSTER_INSPECT, KAFKA_CLUSTER_READ, KAFKA_CLUSTER_CREATE, KAFKA_CLUSTER_UPDATE, KAFKA_CLUSTER_DELETE, KAFKA_CLUSTER_CONFIG_INSPECT, KAFKA_CLUSTER_CONFIG_READ, KAFKA_CLUSTER_CONFIG_CREATE, KAFKA_CLUSTER_CONFIG_UPDATE, KAFKA_CLUSTER_CONFIG_DELETE} in compartment id <VM_COMPARTMENT_OCID>
Allow service rawfka to use vnics in compartment id <VM_COMPARTMENT_OCID>
Allow service rawfka to use subnets in compartment id <VM_COMPARTMENT_OCID>
Allow service rawfka to use network-security-groups in compartment id <VM_COMPARTMENT_OCID>
Allow service rawfka to read secrets in compartment id <VM_COMPARTMENT_OCID>
Allow service rawfka to use secrets in compartment id <VM_COMPARTMENT_OCID> where request.operation = 'UpdateSecret'
```

The service statements support private broker attachments and SASL/SCRAM secret
association. Listing alone only needs the two `INSPECT` permissions. Broker
authentication and topic permissions are separate from OCI cluster IAM: configure
the supported SCRAM user, Vault reference and appropriate broker ACLs. Connect
requires access to its dedicated state/schema topics as well as the data topic.
The implementation uses SCRAM-SHA-512 over TLS on 9092. mTLS is not implemented.
[Oracle Kafka prerequisites](https://docs.oracle.com/en-us/iaas/Content/kafka/gs.htm).

## MySQL grants are separate from OCI IAM

| Account | Required access in this implementation |
| --- | --- |
| Source connector | SELECT, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES; RELOAD or the supported HeatWave FLUSH_TABLES alternative. Validate scope/roles for snapshot locking. |
| Target application/writer | SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER and INDEX on the selected audit schema for layout preparation, evidence, checkpoint/retention state and partition maintenance. DROP is needed only for explicitly used scratch-schema verification, not normal event writes. |
| Target Debezium state | CREATE, SELECT, INSERT, UPDATE, DELETE on its JDBC history and capture-request tables; SELECT on metadata as required by the driver. Current flow configuration reuses its target credential. |
| Local administrator | Socket-only embedded MySQL authentication and repository management; never substitute source/target accounts. |

Database/schema creation and user grants are DBA prerequisites. For tighter
runtime grants, pre-provision the exact tables and validate startup DDL behavior
before removing privileges; separate migrator/runtime target accounts are not yet
implemented. No source SQL or IAM is changed by source validation.
[Source settings and grant checks](before-after-capture.md#source-readiness).

## Verify access without changing IAM

Run `.venv/bin/python scripts/check_setup.py --oci --transport streaming` on the VM
(or `kafka` / `both`). It loads `.runtime.env`, verifies the VM scope, checks selected
resources and lists service inventories without retrieving secret values. A passed
list/get proves that read only: it does not certify create/delete permissions,
container-principal access, broker authentication or end-to-end capture.

For a 404/403, keep the operation, safe OCI code and request ID; verify the correct
domain/group, matching rule, compartment and regional service availability.
Allow IAM changes to propagate, then retry the same identity. Do not broaden to
`manage all-resources in tenancy` or fall back to user/API-key credentials.


See [the same-compartment limitation](compartment-scope.md) for Vault/secret
selection, other OCI resource boundaries and the host-only database identity
exception. Broader IAM access does not expand this application boundary.
