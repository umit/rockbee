# RockBeeRaftClient

The public client for the independent Raft KV workspace. It connects to a
gateway and stores opaque byte keys and values. OpenRaft and RocksDB stay behind
the node boundary; client applications do not depend on either engine.

```text
RockBeeRaftClient -> Gateway -> group leader -> OpenRaft/RocksDB replicas
```

## Reading the API

Start at [api/client.rs](src/api/client.rs). The public methods are `connect`,
`put`, `get`, `delete`, and `close`. [ClientConfig](src/config/mod.rs) holds the
request timeout and connection admission limits.

`put` and `delete` return a `MutationHandle`. For a Java developer, think of it
as an observer of already submitted work, similar to a future returned by an
asynchronous client. The call admits the operation immediately; awaiting the
handle observes its outcome. Dropping the handle does not cancel the write.

`get` is an ordinary Rust `async fn`: its work begins when its future is polled.
It returns `Result<Option<Bytes>, ReadError>`. `Ok(None)` means the key is absent;
`Ok(Some(Bytes::new()))` is a stored empty value. An error is neither absence nor
an empty value.

## Several requests on one connection

The client already supports bounded pipelining and multiplexing: it can send
another request while earlier requests await replies, and request IDs match
out-of-order replies to the right handle. Clones share that same connection.

Poll independent reads together instead of awaiting them one at a time:

```rust
let (product, inventory) = tokio::join!(
    client.get(Bytes::from_static(b"product/42")),
    client.get(Bytes::from_static(b"inventory/42")),
);
```

For writes, `put` and `delete` admit work immediately. Keep the returned handles
and await them together. If a later admission fails, an earlier accepted write
still runs; retain its handle and inspect its outcome. These are independent
requests, not an atomic batch. When ordering matters for the same key, await
the preceding mutation and require `Committed` before submitting the next one.

Request-count and byte limits apply across every clone. Pipelining does not
remove admission errors or the `Unknown` outcome. The three-node soak uses this
same public connection with one cloned client per worker; set
`ROCKBEE_SOAK_CONCURRENCY=8` for eight concurrent workers.

## Write outcomes

| Outcome | What is known |
| --- | --- |
| `Committed` | The node returned the durable consensus acknowledgement. |
| `NotSent` | The local connection proves it never handed this request to its writer. |
| `Rejected` | A server proves it rejected the operation before Raft submission. |
| `Unknown` | The operation may have committed; do not automatically replay it. |

Admission errors from `put`/`delete` occur before a handle is returned. They are
distinct from a later `Unknown`. A request ID correlates replies; it is not a
deduplication or exactly-once token.

Successful GETs use the node's OpenRaft read barrier. After an acknowledged PUT,
a later successful GET returns that value or a later committed change. Loss of
quorum may produce a read error; a follower's stale local value is not returned
as a successful strong read.

## Connection ownership

Cloning a client shares one bounded connection generation. Dropping one clone
does not close the others. Explicit `close().await` retires that shared
connection and joins its workers. A handed request timing out can retire the
connection and make other handed mutations `Unknown` too.

The public client never reconnects or replays automatically. Create a new client
with `connect` after a retired connection, preserving the uncertain outcomes of
the old connection. The gateway can separately reconnect its backend links for
new work proven unsent.

See [the real public-path test](../../apps/rockbee-raft-node/tests/runtime.rs)
and [the three-process recovery test](../../apps/rockbee-raft-node/tests/processes.rs)
for complete executable examples. The
[local configuration guide](../../configs/README.md) starts actual node and
gateway binaries.
