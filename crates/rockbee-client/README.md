# Rust Client

`rockbee-client` is the public async SDK. The CLI lives in a separate
[tool package](../../tools/rockbee-cli). See the
[Client API](src/lib.rs) and [Protobuf schema](../../proto/rockbee.proto)
for API and wire details.

Keys and values are opaque bytes. `Client::put`, `get`, and `delete` require
nonempty keys; PUT also requires a nonempty value. The Client rejects empty
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
Client -> Gateway -- PUT/DELETE -> Writer -> SlateDB -> object store
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
| `Unknown` | The mutation may have succeeded; the Client could not establish the final result. |

Request IDs correlate responses; they are not durable deduplication keys.
Do not automatically retry an `Unknown` mutation. Dropping a PUT handle stops
observing its result; it does not cancel the submitted write. Keep the Tokio
runtime alive until outstanding handles complete.

## Client limits

With `Client::connect`, each connect/request attempt has a five-second deadline.
One multiplexed connection permits 128 in-flight and 128 pending requests across
all `Client` clones. A request's deadline includes any reconnect, admission wait,
registration, and response.

Use `Client::connect_with_config` only when the application needs different
limits. `ClientConfig::new` requires a positive, representable timeout,
`1..=128` in-flight requests, and a positive pending limit that fits Tokio's
semaphore capacity. Invalid settings fail before connecting:

```rust,no_run
use rockbee_client::{Client, ClientConfig, Result};
use std::time::Duration;

# async fn example() -> Result<()> {
let config = ClientConfig::new(Duration::from_secs(2), 64, 64)?;
let client = Client::connect_with_config("127.0.0.1:7100", config).await?;
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

## Graceful shutdown

`shutdown` closes admission for this `Client` and every clone when its future is
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
