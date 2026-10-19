# Server writer lifecycle: red regression evidence

## Scope

This investigation changes no production code. It tests only the private typed
TCP server in `rockbee-raft-protocol`.

## Reproduction

`writer_termination_retires_later_input_but_drains_accepted_work` starts an
owned loopback listener with a 10 ms I/O bound. It accepts fifteen GET requests
whose released handlers each produce a one MiB response. The client does not
read those responses. It keeps the server's input half active with complete,
confirmed requests whose handlers wait without producing output, then sends one
late valid request after one hundred milliseconds.

The confirmation after every held frame separates this from the reader's own
idle timeout: the server has actually decoded and admitted the input stream
while the output writer is blocked. A non-observed held frame is treated as
connection retirement, rather than turning the regression into an unrelated
test timeout. All held accepted handlers are released before server shutdown,
including when the final assertion fails.

```text
CARGO_TARGET_DIR=target/server-debug cargo test -p rockbee-raft-protocol \
  --test server writer_termination_retires_later_input_but_drains_accepted_work \
  --locked --offline -- --exact

FAILED: handler calls changed from 56 to 57 after writer expiry
```

This command needs the local TCP permission available in the development
environment. It completed in 0.16 seconds and failed at the final call-count
assertion.

The integration fixture then splits the same client socket. Its write half
continues confirmed held-request heartbeats while its read half drains buffered
output to EOF. That EOF is the exact connection's response-writer closure, not
an elapsed-time inference. Only after stopping and joining the heartbeat does
the test send a unique late request and compare handler calls. The test remains
red because that late request is admitted.

The writer's termination is also checked directly by the private unit test
`transport::server::runtime::tests::writer_timeout_closes_the_output_half`.
It keeps the response sender alive, writes 16 MiB to a non-reading owned TCP
client, awaits the actual `write_responses` task completion, then observes EOF
after draining the partial output. This passed in 0.01 seconds. A successful
client `write_all` is deliberately not treated as proof of server admission.

## Root cause

`transport/server/runtime.rs::run_connection` creates a response-writer task,
but its intake loop selects only stop, completed handlers, and a pinned receive
future. `write_responses` exits on an output timeout or write failure. Its
`JoinHandle` is observed only after intake already ends, so a live read half can
decode and submit more handler work after output is no longer possible.

## Small fix proposal

Select the response writer's completion beside stop, handler completion, and
the pinned receive future. When it finishes, stop intake immediately, drop the
response sender, and join all already accepted handlers. Track that completion
so cleanup does not abort or await the same completed `JoinHandle` again.

The pinned receive future must be discarded only because the connection is
retired. No accepted handler is aborted: a completed handler's response send
will fail and release its reserved resources normally. This preserves the
existing durable-work rule for requests accepted before writer termination,
while rejecting new input after it.

## Applied fix and green evidence

`run_connection` now selects the mutable response-writer `JoinHandle` before
new receive completion. Any writer completion, including a panic, retires
intake. Cleanup tracks that completed handle so it does not abort or await it a
second time. The response sender is dropped and every already accepted handler
is joined normally.

The regression now records both handler starts and completions. It proves that
some held work remains incomplete before release, then verifies after
`Server::close()` that every admitted handler completed. Thus the new intake
fence does not cancel previously accepted work.

```text
CARGO_TARGET_DIR=target/server-debug cargo test -p rockbee-raft-protocol \
  --test server --locked --offline
7 passed

CARGO_TARGET_DIR=target/server-debug cargo test -p rockbee-raft-protocol \
  transport::server::runtime::tests::writer_timeout_closes_the_output_half \
  --lib --locked --offline -- --exact
1 passed

CARGO_TARGET_DIR=target/server-debug cargo test -p rockbee-raft-client \
  -p rockbee-raft-gateway --locked --offline
10 passed

CARGO_TARGET_DIR=target/server-debug cargo clippy -p rockbee-raft-protocol \
  --all-targets --locked --offline -- -D warnings
finished successfully
```
