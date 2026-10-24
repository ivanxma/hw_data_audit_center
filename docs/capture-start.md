# Capture start and explicit repositioning

Initial table registration offers **Start** and **End**. Start uses Debezium
`initial`: capture current rows as SNAPSHOT records, then stream new changes.
End uses `no_data`: capture the schema baseline without current rows, then stream
changes after the startup boundary. Neither choice reconstructs purged history.
Normal container restart/resume continues the saved connector offset.

Control Center → **Capture start** offers an explicit position operation for a
connector deployed with the current capture-control image:

| Choice | Meaning for an existing flow |
| --- | --- |
| Start | Replay after the earliest retained initial schema baseline |
| End | Capture after the source executed-GTID set observed at preview time |
| Specific GTID | Capture transactions not included in the supplied executed set |
| Datetime | Resolve UTC time to the first retained transaction whose immediate commit timestamp is at or after it |

A specific GTID value is a complete executed set, for example
`aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-120`, not a server UUID filter or an
instruction to include transaction 120. Tagged GTIDs are rejected by this
implementation. A new flow must establish schema history before historical
positioning is available. Start does not mean the first transaction ever executed
on a server; it means the earliest point supported by the retained schema evidence.

The app verifies that the requested set is contained in `gtid_executed`, contains
all `gtid_purged` transactions, and covers the retained schema baseline. This
prevents replay with missing binlogs or a definition captured later than the
requested data. A supplied GTID set is also matched against cumulative transaction
order in retained binlogs; arbitrary holes that could skip a schema change are
rejected. This scan requires the baseline binlog to remain available. Kafka and JDBC history are inspected through separate readers.

Datetime lookup runs the OL9 VM's bundled `mysqlbinlog` over TLS, with a private
0600 temporary defaults file. It scans only transaction metadata, keeps raw row
payloads out of application logs and files, and bounds elapsed time, file count
and output size. UTC is explicit. A source clock change can make timestamps
non-monotonic; the result identifies the first matching transaction boundary,
not a filter that excludes every later event carrying an older timestamp. Purged
baseline files, unsupported timestamps and oversized scans are rejected with
instructions to review the baseline and retained evidence.

## Applying a reviewed position

Preview saves a prepared request in target `audit_capture_requests`; it does not
change the connector. The request belongs to the authenticated actor and flow and
expires after five minutes. Applying it requires acknowledgement that backward
movement replays records and forward movement can skip uncaptured changes.
The source/schema retention checks run again before submission.

Before applying, the requested GTID set must also be covered by the connector's
durable pre-change checkpoint. A forward jump beyond decoded history could skip
DDL needed to interpret later rows, even when the source still has those logs.
Such a request fails with `SCHEMA_HISTORY_COVERAGE_UNCONFIRMED`. Resume the existing
checkpoint, let capture catch up, and preview again; do not bypass this check.

The Debezium container polls its own pending requests and uses Kafka Connect's
loopback REST API. It stops the connector task, saves the previous offset document,
patches the reviewed source offset, reads it back, marks the request applied, and
resumes the task. The OCI Container Instance itself remains running. Credentials
are resolved from same-compartment Vault secrets inside the workload.

A completed/applied request never resets the offset again on restart. A crash
after PATCH is reconciled against the reviewed offset and the saved prior offset.
Ambiguous progress stops for review rather than rewinding again. Failed changes
leave evidence and can leave the connector task stopped. **Resume existing
checkpoint** resumes that stored state without choosing another position.

The operation retains writer consumer groups, target delivery checkpoints,
row-fingerprint keys, audit payloads and schema history. Replayed source DML is
checked against the identity ledger; conflicting evidence is rejected. Snapshot
observations use queue identity and are distinct from source DML.

## Deployment and validation

Build and select the current Debezium image and use **Update Debezium image**
on the selected flow. It stops the old connector, waits for INACTIVE, and
registers its replacement while leaving the writer running. Only builds containing the
matching capture-control runtime receive protocol version 2. Older deployed images cannot
accept historical-position requests. No source position is changed by deployment
alone. Retired flows reject requests.

Automated checks cover GTID normalization, UTC transaction-boundary parsing,
retained-history gates, actor-bound confirmation, snapshot interruption guards,
crash reconciliation and the no-repeat-reset rule. End-to-end offset changes must
also be acceptance-tested on a dedicated authorised test flow before production
rewinds. The app does not silently reposition existing flows during an upgrade.

References: [Debezium MySQL 3.4](https://debezium.io/documentation/reference/3.4/connectors/mysql.html),
[Debezium storage](https://debezium.io/documentation/reference/3.4/configuration/storage.html),
[MySQL mysqlbinlog](https://dev.mysql.com/doc/refman/8.0/en/mysqlbinlog.html),
[Kafka Connect offset-management design](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/231116066/KIP-875%2BFirst-class%2Boffsets%2Bsupport%2Bin%2BKafka%2BConnect).
