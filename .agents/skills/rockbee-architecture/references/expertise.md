# Expertise and decision criteria

Use the relevant sections for the task. These are areas the Database Architect must be
able to investigate, not claims that Rockbee implements every capability.

## Contents

1. Hardware and computer architecture
2. Unix, operating systems, and filesystems
3. Rust, Tokio, and native-library boundaries
4. Algorithms and database internals
5. Storage engines and data lifecycle
6. Networking and protocol engineering
7. Consensus and replication
8. Cluster architecture and control plane
9. Client API and database semantics
10. Time and failure models
11. Performance engineering
12. Benchmarking, statistics, and capacity
13. Correctness and failure verification
14. Observability and production operations
15. Recovery and version lifecycle
16. Security and isolation
17. Product fit and economics
18. Cross-layer questions

## 1. Hardware and computer architecture

- CPU cores, hardware threads, cache hierarchy, cache lines, locality, false
  sharing, atomic memory ordering, memory bandwidth, and NUMA where applicable.
- SSD/NVMe latency, IOPS versus bandwidth, queue depth, endurance, controller
  behavior, and power-loss assumptions; distinguish device claims from measurements.
- Understand ARM versus x86 and macOS development versus Linux server differences
  when interpreting measurements. Do not extrapolate local results into production capacity.
- Investigate hardware effects when a profile or workload suggests them; CPU
  affinity, custom allocators, SIMD, and assembly are conditional techniques.

## 2. Unix, operating systems, and filesystems

- Processes, threads, scheduling, context switches, signals, descriptors, sockets,
  child-process ownership, graceful shutdown, and cleanup after cancellation.
- Virtual memory, page faults, RSS, allocation versus resident memory, page cache,
  memory mapping, dirty pages, writeback, memory pressure, swap, and OOM behavior.
- Buffered/direct I/O, partial I/O, filesystem metadata, rename, directory
  persistence, `fsync`/`fdatasync`, disk-full and I/O errors. Check the actual OS,
  filesystem, and device guarantees rather than assuming one universal contract.
- epoll/kqueue, resource limits, Linux cgroup CPU/memory/I/O isolation and pressure,
  container throttling, and the relationship between DB cache and OS cache budgets.

## 3. Rust, Tokio, and native-library boundaries

- Ownership/borrowing, lifetimes, error contracts, public API design, visibility,
  trait boundaries, and readable modules without unnecessary abstractions.
- Futures and task scheduling, cancellation safety, task ownership, lock scope,
  blocking work, bounded channels, backpressure, and graceful shutdown.
- Allocations, copies, reference counting, memory layout, atomics, synchronization,
  unsafe invariants, and FFI ownership/error boundaries with native RocksDB.
- Cargo workspaces, feature unification, native dependencies, profiles, platform
  differences, and reproducible builds; distinguish compiler settings from runtime tuning.

## 4. Algorithms and database internals

- Hashing and distribution, ordered indexes, B-trees/LSM trees, Bloom filters,
  encoding, serialization, checksums, compression, and complexity under skew.
- Point lookup versus scan, ordering requirements, key/value size distributions,
  locality, index/cache memory cost, and collision/overflow behavior.
- MVCC, locking, optimistic concurrency, isolation, and transaction boundaries
  when evaluating proposed functionality. Explain what a single-key guarantee
  does and does not establish about multiple keys or groups.
- SQL planning, vector indexes, and analytical execution are conditional product
  topics; they do not become Raft-workspace requirements by appearing here.

## 5. Storage engines and data lifecycle

- RocksDB options and state: WAL, memtables, SSTs, flushes, compaction, block cache,
  filters, column families, snapshots, and background resource sharing.
- Read/write/space amplification, compaction debt, write stalls, compression
  cost, steady-state behavior, disk capacity, and flash endurance.
- Durable acknowledgement boundaries, atomic batches, write ordering, sync
  semantics, crash recovery, corruption detection, and snapshot installation.
- Follow data through write, update, delete/TTL, tombstone retention, reclamation,
  backup, and restore. Distinguish logical deletion from physical removal.
- In the original SlateDB product, also consider object-store request/transfer
  cost, caching, writer fencing, eventual reader convergence, and published
  SQL/Parquet projections when relevant. Verify those paths separately from Raft.

## 6. Networking and protocol engineering

- TCP streams, framing, partial reads/writes, malformed/truncated frames, size
  limits, versioning, serialization, and bounded decoding/allocation.
- Multiplexing/pipelining, response correlation, out-of-order completion,
  connection ownership, cancellation, deadlines, reconnects, and socket shutdown.
- Queue limits, flow control, backpressure, fairness, slow clients, request fanout,
  retry storms, and where overload is accepted, queued, or rejected.
- RTT, congestion, packet loss/retransmission, socket buffers, NIC queues,
  DNS, TLS/mTLS, proxies, NAT, load balancers, and health/readiness interactions.
- Trace the client, gateway, node, and peer paths individually; distinguish
  serializer choices such as Protobuf from transport choices such as gRPC.

## 7. Consensus and replication

- Raft/OpenRaft terms, votes, elections, log matching, replication, commit/apply,
  quorum, persistence obligations, and safety versus liveness assumptions.
- Linearizable reads/read barriers, follower behavior, stale leaders, lost
  acknowledgements, leader transitions, and bounded replication work.
- Multi-Raft group isolation and shared resources, learners, membership changes,
  joint consensus, log truncation, snapshot transfer, and crash/restart ordering.
- Distinguish the consensus library's guarantees from obligations in Rockbee's
  storage, transport, state machine, metadata, and public API integration.

## 8. Cluster architecture and control plane

- Key-to-shard/group mapping, placement, leader discovery, routing generations,
  stale metadata, fencing, and control-plane versus data-plane responsibilities.
- Bootstrap, joining, draining, removing, replacing, and restarting nodes;
  group movement, rebalancing, interrupted operations, and operation resumption.
- Hot keys and skew, group count versus node count, shared process budgets,
  failure domains, replication placement, and isolation of slow groups.
- Identify what is static configuration, operator-controlled, or automatic.
  Do not infer automatic failover/rebalancing from the presence of Raft.

## 9. Client API and database semantics

- Define PUT/GET/DELETE success, failure, timeout, cancellation, and disconnect
  outcomes; retain eager completion handles where the API requires them.
- Distinguish not-sent, proven rejection, committed success, and uncertain
  mutation outcomes. Do not automatically replay uncertain mutations.
- Retry eligibility, idempotency/deduplication scope and retention, request
  ordering, connection-scoped IDs, deadlines, and client resource ownership.
- Consistency, durability, atomicity, transaction boundaries, table/KV namespaces,
  deletes/TTL, and compatibility promises. Verify each implemented guarantee.

## 10. Time and failure models

- Monotonic versus wall clocks, clock skew/jumps, timeout propagation, deadlines,
  leases, epochs, fencing, and assumptions about bounded clock/network behavior.
- Process death, machine/power loss, network partitions, slow/stuck processes,
  disk errors/full disks, and corrupted state are distinct failure models.
- A timeout does not prove a node is dead or a write failed. A process-kill test
  does not alone validate power-loss durability or every distributed failure.

## 11. Performance engineering

- Start from workload and latency/error objectives. Identify the limiting
  resource with utilization, saturation, errors, and queue/service-time evidence.
- Use on-CPU/off-CPU, allocation/heap, lock, scheduler, syscall, and I/O profiling
  as appropriate. Choose tools for the actual OS and permissions available.
- Understand queueing, Little's Law and its assumptions, saturation, tail
  latency, contention, serial fractions, and throughput/latency tradeoffs.
- Evaluate batching, concurrency, caches, compression, allocations, and copies
  against measured constraints. Account for profiler/telemetry overhead.
- Establish a baseline, state the hypothesis, change controlled variables,
  compare equivalent conditions, and check correctness plus resource effects.
  Do not silently weaken sync/durability guarantees to report higher throughput.

## 12. Benchmarking, statistics, and capacity

- Specify client/connection/in-flight counts, gateway/node/group topology,
  operation mix, key/value sizes, key distribution, data volume, and network RTT.
- Separate open-loop arrival rate from closed-loop concurrency, warm from cold,
  cache-resident from larger-than-RAM data, and startup from compaction steady state.
- Record throughput, p50/p95/p99 as sample size permits, errors, retries,
  timeouts, CPU, RAM, disk, network, queues, test duration, and configuration.
- Examine coordinated omission, load-generator limits, warmup, repeated-run
  variability, sampling error, and comparable histogram aggregation.
- Include skew/hot keys, mixed workloads, recovery, failover, overload, and
  post-overload recovery when the question requires them; begin with small cases.
- Estimate capacity with explicit assumptions, failure headroom, disk growth,
  amplification, replication traffic, recovery time, and cost. Label projections
  separately from measured sustainable capacity under a stated latency/error target.

## 13. Correctness and failure verification

- Choose unit/integration, property/fuzz, concurrency, and end-to-end tests
  according to the invariant. Reproduce a failure before changing behavior.
- Capture concurrent operation histories and check the promised consistency
  model, including incomplete/uncertain operations and leader or membership changes.
- Inject process/network/disk faults and lost replies at meaningful lifecycle
  boundaries. Verify persisted results and recovery, not just surviving processes.
- Use reproducible seeds, minimized counterexamples, model checking, or
  deterministic simulation where the expected benefit justifies their cost.
- Run isolated tests with unique storage and owned-process cleanup. Private
  fault-injection copies need their own Cargo target directory. Report blocked
  prerequisites separately from product failures and never invent passing coverage.

## 14. Observability and production operations

- Metrics, structured logs, correlation identifiers, and tracing/profiling when
  needed; control cardinality, payload exposure, and instrumentation overhead.
- Make queueing, saturation, retries, write stalls, leader changes, replication
  lag, recovery progress, and rejected/uncertain mutations diagnosable.
- Health versus readiness, startup/draining, load balancing, overload admission,
  retry amplification, alerting, latency/availability objectives, and incident response.
- Diagnose with evidence across application, runtime, kernel, network, and
  storage; preserve useful artifacts and write operational recovery procedures.

## 15. Recovery and version lifecycle

- Backup integrity, consistent exports/snapshots, isolated restore drills,
  disaster recovery, recovery-point/time objectives, retention, and data deletion.
- Replication is not backup: erroneous deletes and corruption can require
  separate recovery paths. Measure recovery time as well as normal throughput.
- Client, wire, config, WAL/disk, and snapshot versioning; mixed-version rollout,
  migrations, upgrade/downgrade compatibility, and explicit rollback boundaries.
- Validate supported upgrade/recovery paths against actual artifacts and data;
  do not assume every format change is backward compatible or reversible.

## 16. Security and isolation

- Deployment-specific threat models, trust boundaries, identities, authentication,
  authorization, transport encryption, certificates, and secret/key rotation.
- Malformed input, decode/allocation limits, resource exhaustion, slow clients,
  tenant isolation where applicable, and permission boundaries for administration.
- Sensitive logs/backups, dependency/native-library risk, and deployment exposure.
  Evaluate controls in proportion to the product and environment.

## 17. Product fit and economics

- Choose complexity in response to an actual use case; consider operability,
  maintenance, portability, developer experience, and implementation cost.
- Separate logical data size from replicated disk, cache/RAM, temporary
  compaction/snapshot space, and backup capacity; account for failure reserves.
- Compare local-disk and shared-object-store designs under their own consistency,
  latency, durability, transfer/request-cost, and operational assumptions.
- Evaluate algorithms/architecture rather than copying a competitor's labels.
  Custom kernels/drivers, RDMA, GPU, or SIMD work needs a concrete product need
  or measured bottleneck; knowing a technique is not a reason to introduce it.

## 18. Cross-layer questions

- After the client receives success, what survives a process crash, a node loss,
  or a power failure? Trace acknowledgement, quorum, persistence, and recovery.
- If the reply is lost, what can the client safely conclude or retry? Trace
  transport correlation, accepted-work ownership, deduplication, and stored state.
- When one disk slows or fills, how do queueing, Tokio tasks, replication,
  quorum availability, deadlines, health checks, and recovery interact?
- During a group move or mixed-version upgrade, who is authoritative and how
  are old leaders, old routes, partial transitions, and rollback handled?
- When throughput rises, what happens to p99, errors, durability, memory,
  disk growth, recovery time, and operating cost?

## Primary references

These sources support the subject areas, not claims about Rockbee's implemented
features. Check the relevant dependency versions and current documentation for decisions.

- [CMU database systems curriculum](https://15445.courses.cs.cmu.edu/spring2025/schedule.html): storage, indexes, concurrency, logging, recovery.
- [MIT distributed systems curriculum](https://pdos.csail.mit.edu/6.5840/schedule.html): replication, consistency, sharding, verification.
- [Linux cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html): operating-system resource control and isolation.
- [RocksDB tuning guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide): measured bottlenecks, compaction, amplification, cache and I/O.
- [Brendan Gregg's USE method](https://www.brendangregg.com/usemethod.html): utilization, saturation, and errors.
- [Jepsen linearizability](https://jepsen.io/consistency/models/linearizable): histories and consistency guarantees.
- [Google SRE cascading failures](https://sre.google/sre-book/addressing-cascading-failures/): overload, retries, and recovery under load.
- [Google SRE data integrity](https://sre.google/sre-book/data-integrity/): detection, repair, and tested restore.
