# Runtime bootstrap and backend review

2026-10-19. Direct advisor/verifier discussion completed before bootstrap
implementation. No design disagreement requiring a Judge. No source edits or
native builds in this review.

```text
BootstrapDirectory -> real group-0 barrier/read -> configured immutable directory
Public KV -> Gateway -> data-group authority -> durable mutation / strong GET
```

## Bootstrap design: approved for the immutable phase

An internal no-payload BootstrapDirectory is appropriate. The receiver validates
destination identity, uses its canonical configured bytes and a real group-zero
linearizable read. Existing identical value succeeds; different/corrupt value
stops bootstrap. Success after writing requires ordinary durable Raft apply.
Public/backend KV cannot target metadata group zero.

After invoking client_write, ForwardToLeader/timeout/lost response is Unknown.
Retain proposal ownership and reconcile with a new linearizable Directory read.
An absent read does not prove an older proposal cannot apply later. Internal
repetition is semantically idempotent only while every potential receiver uses
the same immutable canonical bytes and directory updates are disabled. Duplicate
internal log entries are allowed; this never authorizes public uncertain replay.

Before Task 7 updates directories, use an apply-time put-if-absent/CAS command or
prove old bootstrap proposals/executors retired and drained. Revision one and a
read-before-Put do not fence delayed writes. Concurrent differing configurations
that both read absence are not protected by that check: either enforce the shared
canonical-config precondition or add apply-time protection before claiming such
conflict-race safety. Existing-different rejection alone is narrower evidence.

Required integration checks remain initializer-not-leader, leadership change at
submission, lost response after apply, Unknown followed by absent read/delayed
apply, equal restart bootstrap and existing-different rejection. Bootstrap is
not runtime-verified by this design approval.

## Backend directory and decoder extension: spec/quality approved

- Separate backend operations preserve public/private message-family boundaries.
  KV rejects group zero; response matching verifies cluster, node, kind and group
  where applicable. A leader hint supplies no read authority or replay permission.
- Directory node/voter/placement counts are limited during deserialization before
  vector reservation. Semantic checks enforce uniqueness, endpoint validity,
  voter references, nonzero revision and exact metadata/data group coverage.
- WireRequest's default RESPONSE_DECODE_HEADROOM remains zero for KV. Backend
  supplies 64 KiB for bounded directory representation plus validation scratch.
  RX adds it before frame allocation/decode and retains the permit through an
  unconsumed result. Configuration minima and checked arithmetic include it.
- Inspected the actual failing headroom regression log and the passing full
  protocol/client log: 47 checks pass, including four backend tests and the new
  transport headroom retention test. Existing golden fixtures remain green.
  These runs were inspected, not redundantly repeated. Root records package
  Clippy evidence; later concurrent server files require their own final checks.

## Peer path: approval withheld pending fixes/evidence

Advisor's full-LogId P0 was confirmed and the worker's source fix now preserves
term, committed-leader node ID and index. Requested actual serialized roundtrip
coverage for VoteRequest.last_log_id as well as append/snapshot identities.

The worker is still completing phase-specific idle/body/admission/write deadlines
and shutdown behavior, plus borrowed bounded preflight before DTO payload copies.
The server must reject unknown groups before large body decode, retain submitted
Raft work ownership, and keep saturated bulk admission from monopolizing control
connections. Positive append split hints must represent a fitting nonzero prefix;
a conservative estimate may split early but must not invent Raft success.

No peer approval follows from the earlier checkpoint's 13 tests. The current
report explicitly marks the remaining items unfinished. The worker will provide
a stable code/test checkpoint before independent empirical review; no parallel
native build or mutation of its active files was performed here.
