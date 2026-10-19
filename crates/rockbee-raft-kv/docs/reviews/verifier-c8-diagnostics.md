# Bounded incident instrumentation review

Source-only review; no edits to production, builds or process runs. Diagnostics are feature/env opt-in. No changes to durable ACK classification, mutation replay, configured RPC deadlines, storage sync options or native gate/lane ownership were found in the reviewed checkpoint.

```text
existing RPC / native storage ownership -> bounded diagnostic events
owned node close -> owned watcher join -> Session stderr flush
```

## Required causal correction

At `rpc/network.rs::call`, a decoded `PeerResponseBody::Failure` currently enters `Ok(Ok(response))` and completes the span as success. The OpenRaft wrapper then returns `RPCError::Network` outside that span. The receiving server similarly wraps a Raft error in a Failure response and returns successful transport completion. Consequently a real remote RPC/application failure can disappear from both endpoint error traces.

Record that response's failure message before returning the **same** successful transport response to the existing wrapper, or instrument the final RPC result. Do not change the returned result, remote error mapping or consensus behavior to add diagnostics. Root and Judge received this finding directly. Approval of complete peer-failure coverage is pending this correction; general behavior-preservation review has no blocker.

## Verified ownership and evidence boundaries

- `recording.rs::Span` records failure once by taking its active state. Drop records only a dropped caller span, not a synthesized deadline. An outer upstream timeout can drop it; its cancellation event alone cannot identify why.
- Storage caller and native spans are separate. Native spans move inside actual `spawn_blocking` closures; existing owned lane/gate guards remain there until work ends. Dropping an async waiter does not manufacture native completion or release its permit.
- Group metric observers are held in the runtime JoinSet, abort/joined in close. Watch notifications may coalesce: these are observed term/role states, not exhaustive exact transition timestamps. Session flush follows explicit CLI node close; hot event emission performs no stderr/filesystem writes.
- The recorder preallocates 8,192 event slots; `try_lock` contention/full capacity increments omitted count without waiting. Retained event detail is at most 512 Unicode characters, sanitized for control characters. Error formatting currently calls full `to_string()` before truncation, so the format work/transient allocation is not capped by that retained-string bound. A bounded formatting sink would make the stronger claim true.
- Outbound spans use `record_slow=false`; successful >=25-ms peer calls are omitted. Slow native/storage success can be retained. Do not describe this checkpoint as recording all slow RPC successes.
- Only the final phase and its duration are retained. Total elapsed and final phase cannot fully separate all previous waits. Storage reserve's final phase is `retired_check`, so its total duration is a gate-wait proxy rather than an independently recorded exact gate duration. Inbound connection events currently have group 0/unknown peer/start phase until more context is supplied.
- Peer I/O errors retain the standard I/O error Display, including OS error text/code when supplied by the OS. There is no separately structured raw errno/error-kind field. No request keys, values or full command payloads were observed in diagnostic call sites.

These finite limitations should be stated in the diagnostic run's evidence. A missing event with omissions or no native/role event does not prove the corresponding path did not occur. Source-level wrapper mappings establish policy, while actual event/error classification is still needed to say which path this incident took.
