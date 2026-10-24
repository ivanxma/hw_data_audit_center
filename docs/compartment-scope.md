# Same-compartment resource limitation

HeatWave Audit Center is an independent prototype for evaluation and rehearsal,
not an Oracle product. This installation supports **one VM compartment and the
VM region**. OCI management calls use the VM instance principal. Private runtime
configuration must match the compartment and region reported by the VM metadata
service; selecting another compartment or switching credentials is not a bypass.

This is an **application design restriction**. It is not a statement that OCI
services universally prohibit cross-compartment architectures. Shared resources
in another compartment are outside this prototype's supported deployment model,
even when an IAM policy could permit access.

## What must be in the VM compartment

| Resource or reference | Current application boundary |
| --- | --- |
| Administrator password secret | Select an ACTIVE secret from the VM compartment in Add connection; the app tests the selected credential before use |
| Dedicated Debezium/history-writer password secret | Created in the VM compartment; its username/reference replace the temporary administrator inputs |
| Vault and encryption key | Choose an ACTIVE Vault and enabled AES key in the VM compartment; the key must belong to the selected Vault |
| Existing credential secrets | Use a local-compartment secret and Vault; importing an out-of-compartment OCID is not a supported alternative to selection |
| Row-fingerprint secret | Dedicated, stable secret in the same compartment; preserve it for all existing history |
| OCI Streaming | Stream pool, data/schema streams and Connect configuration in the same compartment |
| Managed Apache Kafka | Cluster/configuration and referenced networking in the same compartment |
| Container Registry | Private repository and selected images in the same compartment |
| Container Instances | Workloads and their selected subnet/VCN attachments in the same compartment |
| Managed networking | Existing VCNs, subnets and referenced NSGs checked by the provisioning path must be in the VM compartment; shared networking is not modified |
| OCI-managed HeatWave DB resources | List/create/start/stop/delete paths are compartment constrained; deletion additionally requires app ownership and dependency checks |

The secret dropdown lists metadata and names, never plaintext secret values.
Empty lists or denied metadata access do not authorize selecting from a different
compartment. Restore the intended VM principal access or create the resource in
the VM compartment. Check which principal needs access: the VM manages/tests
resources, while containers independently resolve credentials and pull images
using their resource principals.

Selecting a compartment-local secret is not a relocation of its existing Vault.
When provisioning existing/shared resources manually, keep both the secret and
its Vault/key in the selected boundary. New account/fingerprint setup explicitly
checks the selected Vault/key attachments. The prototype does not implement a
cross-compartment credential-sharing or secret-migration workflow.

## Database addresses are a separate limitation

Connection registration accepts a database hostname/IP, port, database, username
and selected Vault reference. It tests actual MySQL connectivity/login over TLS.
It does not require a DB-system OCID or infer resource ownership from the address.
**A successful host-only connection test does not prove the database's OCI
compartment.** Operators must place the source/target DB systems in the intended
compartment and verify their placement independently. The OCI HeatWave resource
controls perform resource-ID compartment checks, but those checks do not turn an
unassociated host-only registration into an attested DB-system identity.

## Catalog and IAM distinctions

Shapes, service versions and Oracle-owned default configuration catalogs are
service-supplied choices, not resources created by this app in another customer
compartment. Selecting an Oracle default configuration omits a foreign custom
configuration reference. Tenancy-level IAM groups, dynamic groups and policies
must be prepared by an administrator; the application does not create or change
them.

## Troubleshooting and future scope

1. Confirm the VM compartment/region and configured principal before investigating
   an empty resource dropdown or OCI 403/404.
2. Confirm the resource is ACTIVE/available and located in that compartment.
   Credential lookup requires secret metadata and secret-bundle access; creation
   additionally needs permitted Vault/key operations.
3. Check the VM and worker principal permissions separately. Do not broaden access
   or switch to API-key credentials to avoid the application boundary.
4. Preserve fingerprints, offsets, checkpoints, identity tags and secret values
   during any planned migration. The app does not automatically move/adopt shared
   resources or rewrite existing audit history.

Supporting multiple resource compartments or regions would require an explicit
scope model, separately approved IAM/network access, attachment validation,
credential handling and migration/recovery tests. Editing an OCID in private JSON
alone is not that implementation.
