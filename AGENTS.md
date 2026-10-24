# Repository instructions

## Private documentation and deployment details

- Keep private documentation, deployment-specific runbooks and historical access inventories under Git-ignored `internal_doc/`.
- Scripts must work from any checkout location: resolve supporting files relative to the script, and obtain user-specific local paths from private configuration or environment variables. Never hard-code an absolute local checkout or SSH-key path.
- Public scripts and example configs must use parameters, environment variables, or empty placeholders. Never hard-code real IP addresses, OCI identifiers, private key paths, credentials, or deployment-specific account details.
- Read private deployment configuration through `AUDIT_CONFIG_FILE`. Do not duplicate its values in script source or public documentation.
- OCI mutations must use instance-principal authentication and remain within the compartment explicitly selected in private configuration, in any region. Verify each existing resource's compartment before changing it. Do not change tenancy IAM, shared networking, or resources outside that compartment; denied access is not permission to switch credentials.
- Before committing or publishing, check tracked and newly added files for sensitive deployment values. Git ignore rules do not protect files that are already tracked.

## Container platform

- Run Debezium and history-writer workloads on OCI Container Instances in private subnets.
- Provision and manage OCI resources through the VM instance principal, scoped to its compartment.
- Use container resource principals for workload secret access. Keep images private and digest pinned.
- Store private deployment configuration on the VM and select it with `AUDIT_CONFIG_FILE`.
- Keep runtime registrations and private deployment details under Git-ignored `internal_doc/` or `generated/`.

## Diagram documentation

- Keep architecture documents and READMEs focused on the system architecture and its operation.
- Do not add an "Editing the diagram" section, diagram-authoring instructions, export commands, internal production/verification notes, or messages asking the user to edit draw.io files unless explicitly requested.
- Do not add explanatory messages about draw.io editing merely because an editable source is supplied. Concise artifact links are sufficient when useful.
- Preserve editable diagram sources and rendered previews in the appropriate private or public location according to their contents.

## OCI architecture diagrams

- Read and follow the installed `oci-drawio-architecture` skill at `$HOME/.codex/skills/oci-drawio-architecture/SKILL.md` for any OCI architecture diagram, including those in documents or slides.
- Use the official OCI Library and Oracle architecture toolkit. Use real draw.io region, VCN and subnet containment, correctly parented resources, CIDRs, subnet types, network attachments, routing/security details and relevant DRG/RPC connectivity.
- Keep deployment architecture separate from process diagrams and inspect a rendered preview.

## Presentations

- Use the applicable presentation skill and visual verification workflow.
- Read `$HOME/.codex/skills/oci-drawio-architecture/references/safe-harbor.md` and include its exact paragraph once on a dedicated Safe harbor statement slide, immediately after the title unless the user specifies otherwise.
- When revising a deck, verify an existing suitable statement before adding one. Confirm readability and clipping in renders and exports.
- Oracle styling does not imply Oracle authorship or endorsement.

## reference
- The Log Archiver : https://github.com/ivanxma/heatwave_log_archiver : the logic with fingerprint, source to target.  However Source in the Application is the database table from replication binlog.
- the work flow is Source Connection [HeatWave Source] --- HeatWave binlog → Debezium → Kafka | stream server → history writer → audit-history store[Target HeatWave]

## skill
- Use myapp skills for app menu, flow, security, mysql connectivity, embedded mysql login, Layout with Table, Icon, Action Button, color scheme, TabView
