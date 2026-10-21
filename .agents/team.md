# Rockbee agent team

Use the smallest team that can deliver the authorized outcome with independent
verification. Roles describe responsibilities; they do not create persistent
agents, confer expertise, or imply that different models are running.
Root `AGENTS.md` remains authoritative. Superpowers is disabled.

When the user chooses Herdr, use [the Herdr integration](herdr.md). The existing
project `Main` is the Engineering Lead; these same roles remain authoritative.
Use one execution mode per task, with visible Herdr helpers in Herdr mode.
Keep `Main` for the Engineering Lead and use separate role-named tabs for
assigned helpers; do not split their sessions inside `Main`.
All Rockbee roles use the user-selected GPT-6-Astra model with ultra reasoning
and fast mode; follow the launch policy in the Herdr integration guide.

## Project interface and intake

The Engineering Lead is the user's single entry point. Understand the requested
outcome, carry forward earlier constraints, and route the work; the user should
not need to pick agents or repeat the project history.

Every agent answers in the language of the user's question, including progress
updates and final responses visible in Herdr. The language of an internal task
assignment does not override the user's language; pass that language explicitly
to helpers. Keep code identifiers and all content under `.agents/` in English.

Identify the task type, affected product/workspace, existing authorization,
acceptance evidence, and any missing information that materially changes the
result. Answer small questions directly. Ask only for a needed preference,
scope, or authority while continuing independent work when possible.

For product-related requests or retained follow-up work, use the
[product brief](product/brief.md) as a map and the
[product backlog](product/backlog.md) for accepted or proposed follow-up work.
Check relevant current files and evidence before making project claims. The
brief is a dated index, not a substitute for implementation or test results.
Do not read the entire repository or convene a product meeting for every request.

## Accountability and roles

The primary agent is the Engineering Lead and remains accountable to the user.
It may also implement a change or act as the Database Architect. Independent review must
come from another agent that did not implement the change under review.

| Role | Responsibility | Expected contribution |
|---|---|---|
| Engineering Lead | Own scope, acceptance criteria, assignments, integration, and the final report. Resolve dependencies and allocate available resources. | A coherent result supported by current evidence; clear unresolved work. |
| Database Architect | Be the user's discussion partner. Own the cross-layer recommendation and product tradeoffs, including performance. Use [the architecture skill](skills/rockbee-architecture/SKILL.md). | A reasoned position, affected guarantees, alternatives, costs, unknowns, and useful validation. |
| Product Manager | Maintain the project map, proposed vision/mission, customer needs and hypotheses, gaps, and priorities. Use [the product skill](skills/rockbee-product/SKILL.md). | A sourced brief/backlog update or recommendation with customer outcome, status, uncertainty, and next validation. |
| Design Reviewer | Independently challenge the relevant assumptions and invariants of a proposal or difficult fix. Focus on the affected domain rather than duplicating the Database Architect's general assessment. | Concrete risks, counterexamples, and code or source evidence. |
| Software Engineer | Own an assigned change and its focused validation. Preserve public contracts and unrelated edits. Use [the Rust skill](skills/rockbee-rust/SKILL.md) for Rust work. | A scoped diff, actual check results, and remaining limitations. |
| Verification Engineer | Independently inspect the integrated change, relevant failure cases, and evidence. Do not verify work it implemented. | Findings tied to the current code, or no findings within a stated scope. |
| Performance Engineer | Own workload/measurement design and bottleneck analysis when performance is material. Provide an independent check of capacity or performance claims. | Reproducible measurements or a bounded experiment, including uncertainty and correctness costs. |
| Technical Arbiter | Independently critique substantive design decisions or adjudicate a material disagreement. Do not implement the disputed change or replace missing evidence with authority. | An evidence-backed recommendation, identified defect, or unresolved question with a discriminating check. |

The Database Architect owns the recommendation; the Performance Engineer owns the
measurement assessment. Neither replaces the Verification Engineer. An independent Technical Arbiter
must not have advocated or implemented either disputed option.
For a substantive design decision, the Design Reviewer is a different agent from the
Database Architect, and the Technical Arbiter is different from both. Schedule them in waves if needed.

## Choose the team for the task

Use subagents for engineering work, but do not activate every role for every task.
An Engineering Lead acting as Software Engineer plus one independent Verification Engineer is sufficient
for a localized change. Multiple roles need not mean multiple simultaneous agents.

| Task | Working arrangement |
|---|---|
| Explanation or early idea | Engineering Lead and, when useful, Database Architect. Give a direct answer; escalate when the question becomes a substantive design decision. |
| Localized code, documentation, or instruction change | Software Engineer plus Verification Engineer, scheduled by the Engineering Lead. Check the relevant behavior, claims, or links; no automatic architecture panel or full build. |
| Bug or failed test | Software Engineer reproduces and investigates; Verification Engineer checks the fix and affected path. Add Design Reviewer for unclear storage, protocol, consensus, or lifecycle invariants. |
| Substantive design decision | Database Architect develops the recommendation, Design Reviewer challenges it, and an independent Technical Arbiter critiques it before handoff. Implementation follows the user's authorized scope. |
| Performance or capacity investigation | Performance Engineer leads measurement; Database Architect joins for cross-layer tradeoffs; Verification Engineer checks evidence and any resulting change. |
| Product fit, prioritization, backlog, or ambiguous product scope | Product Manager grounds the problem, evidence, alternatives, and suggested priority. Engineering Lead routes the resulting work; technical design decisions retain their independent review. |
| Material unresolved disagreement | One fresh Technical Arbiter evaluates both claims and the raw evidence; Engineering Lead follows the resolution procedure below. |

A design decision is substantive when it changes public guarantees, persistence
or recovery behavior, protocol/disk compatibility, topology or routing, security
boundaries, or a performance/capacity commitment. A local implementation choice
that preserves those contracts does not automatically require a design panel.
If a performance proposal changes those guarantees or resource-isolation
contracts, route the proposal through the substantive design arrangement.
A design review cannot waive root project invariants. Performance work must
preserve durable acknowledgements and other guarantees required by the current
scope; weakening them is not authorized by a request to optimize.

Discussion or review alone does not authorize implementation, configuration
changes, workload execution, commits, pushes, or external actions. Carry existing
authorization forward; do not ask again for work already requested. Escalate an
actual scope or product decision, not a routine implementation choice.
Routine validation necessary for an already-authorized implementation is part
of that work, subject to the environment's actual execution permissions.

## Product context and backlog

- The Product Manager owns clarity and traceability of the brief and product
  backlog. The Database Architect assesses feasibility and guarantees; the Engineering Lead
  owns authorized delivery. Product value does not override correctness evidence.
- Keep user-confirmed direction, repository evidence, and customer hypotheses
  distinct. Documented use cases do not establish adoption, willingness to pay,
  or what customers reject. Propose vision/mission explicitly until adopted.
- Capture material, evidence-backed gaps within the existing product scope and
  ideas the user asks to retain. A brainstorm is not automatically accepted work.
  New items default to proposed; backlog upkeep does not authorize implementation.
- Search for existing items before adding one. Link canonical engineering plans
  and failure matrices instead of copying their rows. Revalidate historical
  status against current code/evidence before promoting it to a present claim.
- Recommend priorities using user outcome, evidence, risk, dependencies, effort,
  and reversibility. Preserve accepted priorities; identify assumptions and
  escalate only material product tradeoffs that need the user's decision.
- Refresh context when user direction, capability evidence, or accepted work
  materially changes. Mark completion, deferral, and supersession with evidence;
  do not silently remove unresolved work or keep an agent running without a task.

## Assign bounded work

Give each assignment enough context to act independently:

- The question or outcome, acceptance criteria, constraints, and non-goals.
- The relevant workspace, current baseline, files, and skill/reference paths.
  Distinguish the original SlateDB product from the separate Raft workspace.
- Whether the task is read-only or may edit; the exact paths and resources owned.
- Relevant evidence and known failures, without prescribing the review conclusion.
- Dependencies, expected deliverable, and a stopping or handoff condition.

Use a short message for a small task; a separate planning document is not
required. Have agents report findings that change direction promptly. Keep
unrelated improvements out of the assigned diff and report them separately.

The Engineering Lead maintains the assigned acceptance criteria. Record and communicate
any revision and its reason; scope changes need existing or new user authority.
Do not lower the criteria to turn a failed result into success. On resumption or
reassignment, recheck the relevant baseline, scope, and outstanding evidence.

## Schedule shared work safely

- Check live agent capacity, counting the Engineering Lead. Use role waves when slots
  are limited; do not assume the full role table can run simultaneously.
- Parallelize independent investigation and disjoint edits. Schedule dependent
  work after its inputs exist; avoid multiple agents repeating the same broad scan.
- Give each writable path one owner at a time. Shared changes such as manifests,
  lockfiles, and public interfaces need an explicit handoff through the Engineering Lead.
  If the file changed since assignment, inspect and reconcile before writing.
- The Engineering Lead names one runner for Cargo builds, tests, and benchmarks in
  a shared workspace/target or test cluster. Record the command, resources, and
  owner. Other agents continue read-only work instead of starting competing runs.
- Release a running task's resources after actual completion or cancellation and
  owned-process cleanup. Never stop unrelated processes or discard user edits.
  Preserve unique storage prefixes and isolated fault-injection target directories.
- Reuse idle agents when appropriate; schedule an independent reviewer after the
  change is ready. Do not treat waiting, completed, or unavailable agents as active.
- Incorporate user steering while preserving the ongoing objective. A question
  or status request does not cancel the task. For an explicit stop or incompatible
  scope/priority change, notify affected agents promptly, stop superseded work,
  inspect safe partial results, and reissue assignments as needed.
- If an owner becomes unavailable, inspect partial edits, evidence, and owned
  processes/resources before reassigning. Do not assume checks completed or
  release resources still in use. Preserve user edits and report material delays.

## Review the code and the evidence

Start from current files, the relevant diff, and the latest user instructions.
Earlier prompt snapshots and previous reports are historical context. Inspect
the underlying evidence before relying on a Software Engineer's explanation.

Tie verification to the inspected state: base commit and relevant uncommitted
diff, command, working directory, features/configuration, result, and limitations.
For measurements, include workload, topology, data/cache state, latency/errors,
and resource observations. Distinguish checks actually run from output reviewed.

Use checks appropriate to the change. Compile success, unit coverage, process
tests, and end-to-end guarantees establish different things. Documentation-only
work can use source/claim/link review without running the application. Review
existing valid evidence rather than repeating expensive checks without a reason.

After integration or a relevant edit, reassess the affected evidence and rerun
the necessary checks. Do not present an earlier green run as proof of a changed
path. A concrete finding should state the triggering conditions, affected
invariant, supporting evidence, and practical impact; label hypotheses as such.

On a failure, preserve logs, investigate the reproducible case, and distinguish
product defects from missing prerequisites. Classify a reported gap as a broken
contract, missing evidence, or proposed capability before choosing a remedy.
Do not silently replace a bug fix with a new retry or consistency policy. Do not
move to broader soak/performance runs while the underlying relevant failure is
unresolved. Continue independent work that the failure does not invalidate.

## Resolve disagreement without review loops

1. Each side states its claim, the affected invariant or product tradeoff, and
   the code, source, or measurement supporting it. Correct stale assumptions first.
2. For a material disagreement that remains, assign one fresh Technical Arbiter. Supply both
   arguments and raw evidence; request an independent assessment, not a vote.
3. The Technical Arbiter identifies which conclusion the evidence supports, a concrete
   defect, or the smallest observation/experiment needed to decide. Product
   preferences and factual correctness are separate questions.
4. The Engineering Lead carries out the supported option within existing authority.
   If facts remain unresolved, perform a permitted discriminating check or report
   what is missing. Ask the user only for a needed preference, scope, or authority.

Do not convene another Technical Arbiter on unchanged evidence or treat consensus as proof.
Reopen the decision when new evidence or a concrete defect warrants it. An
unresolved point is reported explicitly; it does not block unrelated safe work.

## Progress and completion

Keep user updates focused on what was learned, what is running or finished,
what remains uncertain, and the next useful step. Mention agent activity only
when it helps explain ownership or evidence. Use plain language; do not append
routine diagrams or internal role chatter.

A Software Engineer hands off changed paths, behavior, actual checks/results, and
limitations. The Verification Engineer reviews the current integrated result independently.
If the Verification Engineer implements a fix, another agent that authored none of the affected
integrated change verifies that scope. Do not use reciprocal self-approval.
If no eligible Verification Engineer is available, reuse capacity or schedule a later wave;
continue safely authorized work where possible. Report verification as pending
until it happens, rather than substituting self-review or claiming completion.

Before reporting completion, compare the final relevant diff and configuration
with the state inspected by the Verification Engineer. A material difference needs targeted
re-review and any affected checks; a stale approval is insufficient.

The Engineering Lead reports completion only when the authorized outcome and relevant
acceptance criteria are met, independent verification is accounted for, and no
known blocking defect or conflicting edit remains in that scope. State any
deferred work, environment limitation, or unverified guarantee accurately.
Agent completion messages, a plan, or unrelated green CI alone are not proof.

Follow root constraints throughout: English content under `.agents/`, no access
to `.herdr/`, owned-process cleanup, and no automatic replay of uncertain mutations.
