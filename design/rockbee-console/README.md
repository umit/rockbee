# Rockbee Console — design preview

An interactive, local-only design prototype. All records, service counters, and
query results are sample data. It does not connect to Rockbee or mutate a database.

Open `index.html` through a local static server. No dependencies or build step are
required. Theme preference and preview workspace settings may be stored in the
browser. Preview documents and query history are disposable.

The current visual direction uses the [Rockbee homepage](https://umitunal.net/rockbee/)
palette and typography: paper `#f6f7f4`, ink `#17242b`, blue `#204fe5`, pale blue
selection surfaces, Arial/Helvetica, and system monospace. The code editor and
dark theme use the homepage's slate code-card colors and syntax tokens. The
bee/honeycomb mark remains. Light and dark themes share the same hierarchy and
spacing; the former green/amber interface palette has been replaced.

## Two separate workspaces

```text
Key-value (default) -> exact key -> byte value -> JSON / Text / Hex view
Tables             -> pinned table -> Documents / SQL editor
```

Key-value keeps opaque bytes as bytes. JSON is a display option for a value that
can be decoded as JSON; it does not turn a KV entry into a queryable JSON table.
Copying text preserves the original UTF-8 value; binary values can be inspected
and copied as hex. Sample keys are pinned examples, not a discovered key list.

Tables groups document browsing and SQL under the selected table. Switching
tables keeps a separate SQL draft for each table; history restores its table
context. Table IDs and raw KV keys address separate namespaces.

Services and query history remain secondary workspace tools. The command palette
supports both the button and Command/Ctrl-K, then search, arrows, and Enter.

## Proposed production architecture — not implemented

```text
Browser -> rockbee-console (localhost HTTP) -> RockBeeClient -> Gateway
                                                        |-> Writer -> SlateDB
                                                        `-> Reader -> SlateDB / DuckDB
```

The console should be a separate Rust application with its UI assets bundled
into the binary. Normal data operations use the existing client; diagnostics
query explicitly configured service endpoints. The browser does not speak the
database's TCP protocol or need direct access to object storage.

## Capability boundaries

- Tables are pinned names, not an automatically discovered database catalogue.
- Table browsing can use bounded SELECT queries over the published SQL projection.
- Raw key-value data is an exact-key lookup; the public API has no key scan.
- Status counters describe one endpoint and do not prove cluster/storage health.
- SQL's export watermark is diagnostic; it does not prove immediate visibility.
- Future mutation UI must distinguish durable acknowledgement from reader
  visibility, and must not automatically replay an unknown mutation outcome.

The production backend, real query execution, service discovery, authentication,
and deployment are outside this design spike. They require the next approved
implementation scope.

The [proposed Rust console design](../../docs/superpowers/specs/2026-10-18-rockbee-console-design.md)
defines the next implementation scope, real-data behavior, and acceptance checks.
It is a draft awaiting approval, not evidence of an implemented backend.
