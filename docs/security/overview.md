# Orbit Streams security guidance

Stream records commonly contain tenant data, personal information, credentials, or event content.
Treat values and headers as sensitive even though their repr output omits them. Processors receive
the original bytes and can log, transmit, or persist them; apply data-minimization and retention
rules at the application boundary.

## Validate before acting

Record and batch constructors bound size and identifier syntax but do not validate application
schemas, tenant ownership, authenticity, sequence numbers, or authorization. Parse untrusted payloads
with a bounded schema and reject invalid content before side effects. Do not use a record ID as an
authorization decision unless the application has independently established its trust.

Header values are bounded text without ASCII control characters, but headers have no package-wide
meaning or confidentiality guarantee. Avoid credentials in headers. If records cross a process or
network boundary, authenticate the peer and protect the channel with the host's approved transport
security.

## Diagnostics and plugins

Pipeline exceptions omit processor exception text and suppress the original cause to reduce
accidental leakage. This does not constrain logs created inside processor code, Core, gRPC, or the
Rust plugin. Do not include payloads, keys, sensitive headers, or tenant identifiers in general
metrics labels, traces, or error messages. Apply access controls and retention limits to plugin
diagnostics.

The process adapter runs a separately built executable under Core's process-plugin boundary. Trust
the binary, configuration, and deployment source; constrain executable paths and permissions; and
use Core's supported process isolation and lifecycle controls. Protobuf typing does not authenticate
an arbitrary remote service and this adapter is not a general-purpose network client.

## Delivery and side effects

This package does not make processing durable or exactly once. An application can replay a batch
after failure, and a process timeout can be ambiguous after the implementation performed work.
Use idempotent transformations or durable deduplication before performing externally visible side
effects. Do not commit broker offsets or acknowledge a source until the application has safely
handled the result according to its source/sink failure model.

The package does not encrypt record bytes at rest, manage secrets, provide broker access control, or
apply data-loss-prevention policy. Follow the repository [security policy](../../SECURITY.md) for
vulnerability disclosure; avoid placing sensitive records in public issues or test fixtures.
