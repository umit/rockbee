# Rockbee Herdr integration

Use this guide only for user-authorized Herdr setup or coordination. Follow the
installed Shipmate workflow where compatible with the user's choices and root
`AGENTS.md`. The role definitions in [team.md](team.md) remain the source of truth.

## One project lead

Reuse the current Rockbee workspace, `Main` tab, and matching live coding-agent
session. The Engineering Lead is that Project Main; do not create another project
manager or restart an occupied session to change its label. A general coordinator,
if present, handles project routing rather than directing Rockbee's builders in
parallel with Main.

Confirm `HERDR_ENV=1`, then read `herdr --skill`, current CLI help, and live IDs
before control. Inspect actual cwd, identity, and foreground work. Preserve other
projects and conversations. Never assume that a label proves a parent space or
that a missing API exists.

## Current user-selected layout

Keep `Main` for the Engineering Lead. Put each assigned helper in a separate tab
named for its role, such as `Database Architect`, `Design Reviewer`, or
`Technical Arbiter`. The user prefers full-size role tabs over split panes inside
`Main`. This supersedes the earlier single-tab layout. Do not recreate the
removed permanent Review, Testing, and Delivery stage tabs; workflow stages do
not require their own tabs.

Inspect live identities and cwd before creating or reusing a tab. To move an
existing helper, use `herdr pane move` with `--new-tab`, an explicit workspace,
a role label, and `--no-focus`; preserve its session and continue with the
returned identifiers. Give the pane the same visible role label with
`herdr pane rename`; the registered agent name alone does not label its pane.

Keep completed helper results available for the user to inspect. Do not close
user tabs or restart occupied sessions for cosmetic changes. Setup alone does
not start application work, old TODOs, benchmarks, or eight idle model sessions.

| Responsibility | Herdr execution |
|---|---|
| Engineering Lead | The existing project `Main`; scope, scheduling, integration, and reporting |
| Software Engineer | A bounded visible builder session when implementation is assigned |
| Verification Engineer | A separate visible reviewer with no authorship in the reviewed change |
| Database Architect, Design Reviewer, Technical Arbiter, Performance Engineer, Product Manager | Scoped helper assignments when their expertise is relevant; reuse capacity in waves |
| Test and build commands | Owned terminal processes scheduled by Main; not an extra LLM role |

## Capacity and authority

Use one active project run, Main plus at most two active helpers by default.
Count active, unknown, and blocked helpers before assigning more work. Use
OpenAI-backed coding agents with the user-selected Rockbee policy:
`gpt-6-astra`, `model_reasoning_effort=ultra`, `service_tier=fast`, with
`features.fast_mode=true`. Apply this to Main and every helper; it overrides
older profile values and the installed workflow's default model. The same
defaults are stored in [the project config](../.codex/config.toml). Verify the
live model/effort/speed display after launch; do not silently substitute a model
or restart an occupied session. A role name
does not imply a distinct model or validated expertise.

In Herdr mode, use Herdr-visible helpers. Do not also spawn an invisible native
subagent tree for the same task, and do not let helpers create their own teams.
Include the user's question language in every assignment; visible progress and
results must use that language, even when the coordination prompt is in English.
Keep independent verification and the evidence-based arbitration rules from
`team.md`. Design responsibilities may be scheduled in successive waves.

## Local records and handoff

Root `AGENTS.md` forbids access to `.herdr/`. Do not read, search, modify, execute,
or publish anything there. This restriction overrides the installed workflow's
record-location convention; Herdr CLI session coordination does not authorize
access to that directory. Preserve its local contents.

Main owns current assignments, scope, and evidence. Use the conversation and,
when a temporary handoff artifact is necessary, a task-owned directory under
`/private/tmp`. Keep reusable roles in `.agents/` and product outcomes in the
existing brief/backlog. Do not duplicate the product backlog or create a
competing permanent task board.

Verify live identities/layout and concrete results after changes. Record missing
capabilities honestly; a workspace label is not proof of placement in a space.
Workflow configuration does not establish application correctness or authorize
commits, pushes, or unfinished product work.
