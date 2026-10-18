# Rust RockBeeClient

`rockbee-client` is the public async SDK. The primary type is `RockBeeClient`. The CLI lives in a separate
[tool package](../../tools/rockbee-cli). See the
[RockBeeClient API](src/lib.rs) and [Protobuf schema](../../proto/rockbee.proto)
for API and wire details.

Keys and values are opaque bytes. `RockBeeClient::put`, `get`, and `delete` require
nonempty keys; PUT also requires a nonempty value. The RockBeeClient rejects empty
inputs locally before copying, admission, or sending a request. This is an SDK
input rule. Gateway, writer, and reader also reject raw empty-key CRUD requests
and empty PUT values before forwarding or storage; remote mutation errors are
`Rejected`. The wire version and message layout stay unchanged.

PUT remains eager: `put(key, value, ttl)` returns a `PutHandle` immediately.
An invalid PUT returns a handle whose awaited error has `MutationOutcome::NotSent`;
invalid DELETE also returns `NotSent`. GET uses the generic `Result` error.
Existing size and TTL checks still apply. A valid mutation's successful
completion means durable writer acknowledgement; failed mutations are not
replayed automatically.

```text
RockBeeClient -> Gateway -- PUT/DELETE -> Writer -> SlateDB -> object store
                 `-- GET ------> Reader -> SlateDB -> object store
```

Readers may lag the writer. The response decoder still distinguishes a missing
value (`None`) from a legacy stored empty value (`Some(vec![])`); the new input
rule does not change that compatibility.

Mutation errors carry an outcome:

| Outcome | Meaning |
| --- | --- |
| `NotSent` | This attempt did not send the mutation to the server. |
| `Rejected` | A responding server confirms that it did not apply this request. |
| `Unknown` | The mutation may have succeeded; the RockBeeClient could not establish the final result. |

Request IDs correlate responses; they are not durable deduplication keys.
Do not automatically retry an `Unknown` mutation. Dropping a PUT handle stops
observing its result; it does not cancel the submitted write. Keep the Tokio
runtime alive until outstanding handles complete.

## RockBeeClient limits

With `RockBeeClient::connect`, each connect/request attempt has a five-second deadline.
One multiplexed connection permits 128 in-flight and 128 pending requests across
all `RockBeeClient` clones. A request's deadline includes any reconnect, admission wait,
registration, and response.

Use `RockBeeClient::connect_with_config` only when the application needs different
limits. `ClientConfig::new` requires a positive, representable timeout,
`1..=128` in-flight requests, and a positive pending limit that fits Tokio's
semaphore capacity. Invalid settings fail before connecting:

```rust,no_run
use rockbee_client::{RockBeeClient, ClientConfig, Result};
use std::time::Duration;

# async fn example() -> Result<()> {
let config = ClientConfig::new(Duration::from_secs(2), 64, 64)?;
let client = RockBeeClient::connect_with_config("127.0.0.1:7100", config).await?;
# client.shutdown().await;
# Ok(())
# }
```

These settings affect the client only. Increasing its deadline does not extend
the gateway's independent upstream deadlines or change a server's capacity.

The optional TTL is a `Duration`; `None` or zero means no expiry. The writer
calculates expiry using its clock, and readers filter expired values when
reading. Expiry is logical visibility, not immediate physical object deletion.
See [how it works](../../README.md#how-it-works) for the eventual reader model.

## Diagnostics

The RockBeeClient and its TCP transport emit `tracing` diagnostics without installing
a global subscriber. Configure `tracing-subscriber` in your application's entry
point to view them. With its `env-filter` support enabled, a filter such as
`RUST_LOG=warn,rockbee_client=debug,rockbee_protocol=debug` shows client shutdown
and connection events. Successful requests and key/value contents are not logged.

## Graceful shutdown

`shutdown` closes admission for this `RockBeeClient` and every clone when its future is
first polled. It waits for already admitted PUTs, including PUTs whose handles
were dropped, then disconnects the TCP link. Start eager PUTs before shutdown
and collect each retained result afterward:

```rust
let first = client.put(b"first", b"one", None);
let second = client.put(b"second", b"two", None);

client.shutdown().await;

let results = [first.await, second.await];
for result in results {
    result?;
}
```

`shutdown().await` only means draining and disconnecting finished; it does not
make every mutation successful. Check each `PutHandle`, preserve `Unknown` as
unknown, and do not replay it automatically. A started GET or DELETE must keep
being polled concurrently with shutdown (for example in a task or `join!`) or
be dropped. Canceling a shutdown future leaves admission closed and accepted
PUTs running; call `shutdown` again to finish draining. Keep the Tokio runtime
that called `connect` running until shutdown completes.

## JSON tables and SQL

`RockBeeClient` is the public SDK type.
Byte CRUD continues to work on the same connection. A table handle shares the
client's admission bounds and shutdown gate:

```rust,no_run
use rockbee_client::{Result, RockBeeClient};
use serde_json::json;
use std::time::Duration;

# async fn example() -> Result<()> {
let client = RockBeeClient::connect("127.0.0.1:7100").await?;
let products = client.table("products");
let pending = products.put(&json!({"name":"Keyboard", "price":99.5}));
println!("submitted {}", pending.id()); // UUIDv7 is available before awaiting.
let id = pending.await?; // Durable writer acknowledgement; returns the UUID.
products.put_key("sku-1", &json!({"name":"Mouse"}), None).await?;
let expiring = products.put_with_ttl(&json!({"name":"Offer"}), Duration::from_secs(60));
let expiring_id = expiring.await?;
let document = products.get(&id).await?; // May still be None while the reader lags.
let result = products.query(
    "SELECT id, value->>'name' AS name FROM products ORDER BY id LIMIT 10"
).await?;
products.delete(&expiring_id).await?;
client.shutdown().await;
# Ok(())
# }
```

Documents must be JSON objects. Table names are ASCII identifiers of 1–64 bytes;
IDs contain 1–1024 UTF-8 bytes. SQL must be nonempty, at most 64 KiB, and contain
no NUL. Operations validate these inputs locally before admission; invalid PUTs
and DELETEs return `NotSent`. SQL is passed verbatim to the reader. Its table
has `id VARCHAR` and `value JSON` columns, so use JSON expressions to select
document fields. `QueryResult.rows` contains JSON objects keyed by the selected
column names; `exported_wal_file_id` reports the published export boundary.

```text
RockBeeClient -> Gateway -- table PUT/DELETE -> Writer -> SlateDB -> object store
                        |                          `-> Parquet export -> object store
                        `-- table GET/SQL --> Reader -> SlateDB / DuckDB Parquet
```

GET readers can lag durable mutations. SQL queries the reader's published Parquet files through DuckDB without a fresh
full source scan for each query. It additionally waits for Parquet publication; its results need not immediately reflect a PUT, DELETE, or expiry.
Poll reads for eventual convergence. The runnable
[`tables` example](examples/tables.rs) uses `GATEWAY_ADDR` and waits for its new
products to appear in a real SQL query.

Both auto-ID variants generate UUIDv7 IDs with a client-side timestamp. These
IDs sort by their encoded timestamp; they do not establish distributed commit
or reader visibility order. Caller-supplied IDs, including existing UUIDv4 IDs,
remain valid.

Both auto-ID variants are eager, including serialization and submission before
returning the handle. A dropped `TablePutHandle` keeps running, and `shutdown`
drains it. `put_key` supports caller IDs and replacements and returns the same
`PutHandle` as byte PUT. Mutation outcomes retain `NotSent`, `Rejected`, and
`Unknown`; neither table operations nor shutdown automatically replay a mutation.
