# Dedicated database accounts

This application is an independent prototype for evaluation and rehearsal, not an
Oracle product or an Oracle-supported service. Debezium and the history writer
can use dedicated accounts without MySQL `SUPER`.

## Create an account while adding a connection

1. Open **Configuration & setup → Connections → Add connection**. Fill in the
   endpoint, default database and role. Enter the setup administrator's username
   and select its existing password secret by name from the Vault secret dropdown.
2. Tick **Create Debezium user** for a Source connection, or **Create history
   writer user** for a Target connection. The username/secret fields above are
   labelled as temporary setup-administrator inputs.
3. Enter a different dedicated username and a new password, a MySQL account host
   pattern, and select a same-compartment Vault and enabled AES encryption key.
   `%` permits any client host that can reach MySQL; use a narrower host pattern
   when appropriate, permitting both VM checks and the worker containers.
4. Choose **Load databases**, which connects using the administrator reference.
   Tick the required databases. The connection's default database must be included.
   The list is loaded from live metadata and supports multiple selections.
5. For Target only, optionally enable **Allow partition preparation and cleanup**.
   Review its additional grants before proceeding.
6. Choose **Create and test dedicated account**. The application stores its
   password directly in OCI Vault, creates a TLS-required MySQL account, applies
   the fixed grant plan and checks the resulting grants and dedicated login.
7. On success, the dedicated username and new secret OCID replace the administrator
   inputs in the connection form. The password input is cleared. **Register
   connection** tests the dedicated login again and saves only its username,
   endpoint and Vault reference. The administrator is not registered or passed to
   either workload.

Unchecking account creation preserves the existing workflow for registering an
already-provisioned account. Account creation is offered on Add connection;
existing registration Edit restrictions and already-deployed credentials remain
unchanged. The app does not silently migrate or restart existing flows.

Both the administrator secret and the new credential secret/Vault must stay in the VM compartment. The dropdown lists ACTIVE secret metadata only. See the [same-compartment limitation](compartment-scope.md), which also applies to other managed OCI resources.

## Grant plans

| Account | Scope | Privileges | Purpose |
| --- | --- | --- | --- |
| Debezium | Global | REPLICATION SLAVE, REPLICATION CLIENT | Read binlog and position metadata |
| Debezium | Global | SHOW DATABASES, FLUSH_TABLES | Metadata discovery and supported snapshot/schema-baseline locking |
| Debezium | Each selected database | SELECT, LOCK TABLES | Schema/current-row discovery and snapshot fallback locking |
| History writer | Each selected target database | SELECT, INSERT, UPDATE, DELETE, CREATE | Audit images, deduplication, schema history, capture-control records, checkpoints and target preparation |
| Optional target maintenance | Each selected target database | ALTER, DROP | Prepare/reorganize partitions and drop expired partitions through the guarded cleanup workflow |

No plan grants `SUPER`, `ALL PRIVILEGES`, `CREATE USER`, `GRANT OPTION` or
replication privileges to the target account. The administrator must already have
`CREATE USER` and grantable access to the listed privileges. A server that cannot
grant `FLUSH_TABLES` is rejected; setup does not silently substitute a broader
privilege. The application does not issue `FLUSH PRIVILEGES` after `CREATE USER`
and `GRANT`.

The target account is also used by the app for audit preparation/browsing and,
with OCI Streaming, Debezium's JDBC schema-history/capture-control store.
Consequently `CREATE` supports the current application workflow, even when the
writer itself primarily performs DML. These are the grants for this combined
account model; separate bootstrap, read-only reporting and maintenance identities
are not implemented. Without the optional maintenance grants, ingestion and
initial table preparation work, but partition maintenance/cleanup cannot be
performed with that account.

**Scope limits matter.** MySQL replication access is global: scoped `SELECT`
grants and Debezium table filters do not turn binlog replication into a per-schema
security boundary. `SHOW DATABASES` also exposes database names. MySQL's `DROP`
privilege permits dropping tables/databases in the granted scope, and is required
for `ALTER TABLE ... DROP PARTITION`; it cannot be limited to partition deletion.
Use dedicated audit databases and grant the optional maintenance access only when
that account needs it. The application still enforces its own cleanup policy,
ownership, stopped-workload and retention checks.

On servers with `partial_revokes=OFF`, database-grant patterns containing `_` can
match more than one database. Account setup refuses such selected names rather
than issuing a broader grant; use the HeatWave setting `partial_revokes=ON` or
provision reviewed grants manually. Protected system databases are excluded from
target account setup. Source access to service-managed audit schemas remains
subject to the service's grant restrictions.

## Secrets, retries and failure recovery

- The new password travels over application HTTPS and MySQL TLS. It is not
  returned by the API, persisted in the embedded repository, or written to app
  logs or setup-state files. Container configuration contains only the dedicated
  username and secret reference; the workload resolves Vault at runtime.
- The VM instance principal needs same-compartment access to create/read the
  credential secret and use the selected encryption key. Worker resource
  principals separately need access to the resulting secret. VM access does not
  establish container access. No tenancy IAM or shared network is changed.
- Setup uses a private operation identifier and tagged Vault secret. Non-secret
  progress is stored under Git-ignored `.data/account-setups/` with mode `0600`.
  The administrator username/password reference is not copied into that state.
- MySQL account DDL and Vault creation are not a distributed transaction. A
  partial failure can leave a secret or an app-created account. The safe error
  names the failed step and available secret reference. Retrying unchanged form
  fields resumes that setup; a completed retry tests the dedicated account.
- Existing unmanaged accounts are never reset or adopted. Retrying a partial
  setup requires its matching MySQL account attributes and installation identity.
  Unexpected grants or roles stop setup; unrelated privileges are not revoked.
- After a page reload loses the form's operation identifier, inspect the private
  setup record and the named secret. A verified completed account can be
  registered using the existing-account workflow. Do not repeatedly create new
  identities to bypass a failure, or delete a user/secret still referenced by a
  connection or workload. Password-policy failures can require a new setup with
  a compliant password; review unused partial artifacts separately.

## References

The source plan follows the
[Debezium MySQL account requirements](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-creating-user)
and HeatWave's supported narrower `FLUSH_TABLES` privilege in
[Default MySQL privileges](https://docs.oracle.com/en-us/iaas/mysql-database/doc/default-mysql-privileges.html).
The maintenance permission boundary follows
[MySQL privilege definitions](https://dev.mysql.com/doc/refman/8.4/en/privileges-provided.html).
