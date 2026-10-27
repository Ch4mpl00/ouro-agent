# 10. Reflexes: a catalog the loop can run

Status: reviewed (Codex Sol, two rounds) · Issue: #64 (epic)

## Problem

The agent repeats the same deterministic sequences inside its loop: the news
digest's eight calls ran in a fixed order in every dry run. The model spends
turns on plumbing and makes plumbing mistakes, such as `sinceISO` with `>` or
`query` together with `queries`. The user wants **reflexes**: small saved
workflows that do one finished thing, which the agent sees in a catalog and
runs from its loop ("что там с X в новостях" runs `news-about`). Other
workflows compose them, and a reflex may call a reflex. Later, a dreaming
task proposes new ones from repeated traces; that is out of scope here.

A workflow can already start another as its child (`start_child` with
`host: inherit`, `listen`, `child_outcome`; design 09). Three things are
missing:
- a catalog;
- a way for the loop to run a reflex;
- a guard on what a reflex will do.

## Decisions

1. **A reflex is an ordinary workflow with a declaration.**
   - **The declaration.** `metadata.reflex: { description, input: { <param>:
     <description>, ... }, required: [<param>, ...] }`. The description says
     what it does and when to use it, in one or two sentences. The input
     names its parameters, which the YAML reads as `$workflow.input.<param>`.
     Its output is the workflow's output.
   - **The gate checks the declaration.** The description must not be empty,
     the parameter names must be identifiers, and `required` must be a
     subset of `input`. A reflex must not read `$workflow.input.env`: a
     reflex receives everything through its parameters.
   - **The rest is unchanged.** Versions, `try_workflow`, activation after
     the user confirms, and the breaker all work as for any workflow. No
     engine change.

2. **The catalog in the loop's instructions.**
   - **What the block lists.** A "Reflexes" block lists every reflex whose
     active version was tried, one line each: name, description, parameters
     (required ones marked), and whether it writes (decision 4).
   - **The cap.** The block is capped at 30 lines; past that it ends with
     "more: `list_workflows`".
   - **When it is built and fixed.** The host builds the block when the
     loop's instructions are prepared (`prepare`). It belongs to the
     instructions the run admitted, or to the stored preparation of a
     design 09 child. A restart therefore sees the same catalog, even if a
     reflex was activated in between.
   - **`list_workflows`.** It returns each workflow's `reflex` declaration.
   - **`orchestrator.md` gets one line:** before doing by hand a task a
     listed reflex covers, run the reflex.

3. **`run_reflex(name, input)`, a loop tool that works like a worker.**
   - **Where it goes.** A `run_reflex` call takes the path `invoke_sub_agent`
     takes in `agent-loop.yaml`: `start_worker` → `wait_worker` /
     `sleep_worker` → `tool_repeat` with `worker_run`. `start_worker`
     learns a second kind of call.
   - **How it starts.** For `run_reflex` it checks the input against the
     declaration: unknown parameter → error, missing required → error; the
     model reads the error as the tool reply and nothing starts. It then
     starts the reflex's active version through `start_named_child`, a host
     child of the loop run keyed by the round's call, as workers are. The
     child's host data is the loop run's own `LoopHost`, unchanged. The
     reflex's MCP tools and model steps are therefore served, and its
     confirmations go where the loop's go.
   - **The tool reply.** A `Completed` output is presented like any tool
     result: stored in working memory, with a preview when large. `Faulted`
     becomes `[run_reflex error] <detail>`.
   - **No fallback.** A set-aside or refused start (tripped, paused,
     missing, not a reflex) is an error reply. No built-in stands in.
   - **Recovery.** The child id is deterministic, so a restarted round joins
     the same run and never starts a second one.
   - **The trace.** The reflex's trace nests under the call's span (`anchor_child`), as a worker's does.

4. **The guard sees what a reflex will do.**
   - **What the guard reviews today.** YAML's direct calls are unguarded
     (#59; an activated workflow is trusted code). The guard reviews the
     model's calls.
   - **What a `run_reflex` call is.** Since a reflex runs because the model
     chose it, the call is classified by the active version's YAML. It is a
     **read** if every function the YAML calls is a read (an MCP tool marked
     read-only, a model step, `hash_value`, another reflex that is itself a
     read; recursively, cycles refused). Otherwise it is a **write**.
   - **Read reflexes** run in the round's ready branch with no guard
     request.
   - **Write reflexes** go through the guard like any write. The guard
     request for the call carries the reflex's YAML (the active version, by
     evidence digest, cut to a limit) and the input. The guard's existing
     rules apply ("unsure → ask the user").
   - **A new held-branch path.** The held branch gets the same start/wait
     steps for `run_reflex` that the ready branch has for workers.
     `start_worker` starts the reflex only when the call's verdict
     (`resolve_review` through `guard_run`) allows it; otherwise it answers
     `{id: null}`, and `tool_repeat` presents the verdict.
   - **A write reflex runs at most once per call.** The deterministic child
     id makes the start its effect.

5. **Composition in YAML.**
   - **How a reflex calls a reflex.** With the three standard tasks:
     `start_child { name, host: inherit, input }`, `listen` on `done`,
     `child_outcome`.
   - **Admission.** design 09's preparation applies only to a child whose
     input has `request`. A reflex child inherits host data and runs no
     loop, so the registry must serve it, and test 3 pins that.
   - **Depth.** Nesting is bounded by the engine's depth limit and the jump
     budget. The read/write classification of decision 4 follows these
     calls.
   - **`workflow-author.md`** gets a "Reflexes" section: the declaration, the
     call pattern and a short example (a read reflex calling another).

## Tests (test-first)

1. **A read reflex answers in the same round.** The model calls
   `run_reflex("news-window", {...})`, its output is the tool reply, there
   is no guard request, and the trace nests under the call's span.
2. **A write reflex goes through the guard.**
   - The guard request carries the reflex's YAML and the input.
   - Block → nothing starts. Allow → it starts once.
   - A restart in the middle of the reflex joins the same run; the write is
     sent once.
3. **A reflex calls a reflex.** `news-about` → `news-window` (`start_child`,
   `host: inherit`). The grandchild's MCP calls are served and the output
   reaches the loop.
4. **The catalog.**
   - An active, tried reflex is listed with its parameters and write flag.
   - An untried or inactive one is not.
   - A restart keeps the admitted block.
5. **Bad input.** An unknown or missing parameter is an error reply, and no
   run starts.
6. **A refused start.** A tripped or paused reflex is an error reply, and no
   other version runs.
7. **The gate.** A reflex without a description, or one reading
   `$workflow.input.env`, is refused.

Hand mutants:
- a write reflex classified as a read (test 2);
- `start_worker` ignoring the verdict (test 2's block case).

## Out of scope

- Dreaming that proposes reflexes.
- Macros or Steps.
- Rewriting the digest onto reflexes; the agent does that after this lands,
  as it proposed.

## Revisions after review round 1 (Codex Sol)

These replace the decisions above where they conflict.

R1. **One resolution per call, pinned.**
- **Classification records a resolution.** `classify` resolves the reflex
  once per call and records it in its own output, which the journal keeps:
  - the active version and its digest;
  - the declaration;
  - the transitive summary (decision R2).
- **Every later step reads that record.** Validation, the guard's evidence
  and the start all use it. The start admits **that version**, never "the
  active one" again.
- **Redispatch joins first.** A restarted round finds the existing child by
  its deterministic id before it looks at declarations or activation state.
- **The window for nested reflexes.** A nested reflex that the outer YAML
  starts with `start_child` resolves its active version later, so the
  summary computed for it can go stale. Under "an activated workflow is
  trusted code" this window is accepted and documented: only an activation,
  which the user confirms, can change a nested version in it. The guard
  reviews the model's choice of the outer reflex at its pinned version.

R2. **Two transitive properties: effect and open world.** The summary
computes them over the YAML's calls.
- **What each call is:**
  - **MCP tools:** read/write from `readOnlyHint`, open-world from
    `openWorldHint`.
  - **Pure model adapters**, named explicitly as closed-world reads:
    `llm_compose`, `filter_relevant`, `choose`, `hash_value`.
  - **`set_memory`:** a write.
  - **A nested `start_child` of a literal reflex name:** its own summary,
    recursively.
  - **Everything else is conservative: write + open-world.** This covers
    unknown functions, a non-literal child name, a cycle, and any other host
    function.
- **What needs the guard.** Only a closed-world read bypasses it. An
  open-world read goes to the guard like a direct open-world read
  (`needs_guard`), and so does a write.

R3. **`run_reflex` is its own call kind, not a worker.**
- **Starting: `start_reflex`.** A new host function, not `start_worker`.
  - **Ready branch.** `agent-loop.yaml` gets a `start_reflex` /
    `wait_reflex` / `sleep_reflex` path for `run_reflex`, beside the
    worker's.
  - **Held branch.** The held branch gets the same path, after
    `wait_guard`.
  - **What it starts.** It admits the pinned version (R1) through a new
    `Site::Reflex`. That site checks the target is a reflex and never
    substitutes another workflow: set-aside, refused or missing is a durable
    error result.
  - **Trace.** It anchors the child's trace under the call's span.
- **The result.** `tool_repeat` receives `reflex_run` (as `worker_run` today)
  and presents the child's outcome. `runs_once` exempts a joined `run_reflex`
  as it exempts a joined sub-agent: the start already was the effect, and a
  `tool_once` never runs for it.
- **A call that never starts.** It is a recorded result that `tool_repeat`
  presents as an error reply: blocked by the guard, invalid input, or
  refused.

R4. **The verdict has one owner.**
- **The start spends it.** For a guarded `run_reflex`, `start_reflex`
  spends the call's verdict (`guard_child_verdict`, under its own
  idempotency key) before admitting the child. It records the verdict in
  its result and in the child's admission (host data note).
- **The join only checks.** `tool_repeat` for `run_reflex` never spends a
  verdict again. It checks that the child is the caller's own, and that its
  admission carries an allowing verdict, then reads the outcome.
- **Two crashes to cover in tests:**
  - after the verdict is spent but before the child is admitted: the
    redispatch replays the spent verdict and starts the child once;
  - after admission but before the starter's reply is checkpointed: the
    redispatch joins the child.

R5. **`try_workflow` takes reflex input.**
- `try_workflow(name, version, input)` passes `input` to a reflex draft,
  validated against its declaration, instead of `{env, signal}`.
- A reflex is "tried" only by a dry run with input that passed that
  validation.
- Test: a required parameter that determines a write's arguments shows in
  the dry run's recorded write.

R6. **The catalog reads active versions only.**
- **Eligibility comes from the active version.** It must be active, tried
  (the `workflow_meta` row of that digest) and declared. The declaration
  also comes from the active version.
- **`list_workflows`** returns the active version's `reflex` declaration and
  summary separately from the latest version's.
- **Test:** v1 active and tried, v2 an untried draft with other parameters
  → the catalog shows v1.

R7. **Trace anchoring for inherited non-loop children.**
- **Anchoring no longer depends on loop preparation.** The first agent host
  function that serves a run admitted by a workflow with inherited host
  data anchors it (`run_trace` insert-or-get), under the parent run's
  current span, whether or not it is a loop.
- **Test 3 also asserts the trace.** The grandchild's spans and their
  ancestry survive a restart.

Tests 1–7 stand, with these additions:
- test 2 runs through settlement (block and allow), plus R4's two crashes;
- R2 adds a reflex wrapping `fetch_url`, which goes to the guard as an
  open-world read;
- R5 and R6 add their tests above.

## Review round 2

One medium finding, applied to R7. An intermediate workflow may call no
agent function (only `start_child` / `listen` / `child_outcome`), so it is
never anchored, and its descendant's anchor would have no parent trace.

Anchoring therefore walks up: before anchoring a run, missing ancestor
anchors are established recursively through the caller's `RunReads`, up to
the first ancestor that has one. Each is insert-or-get, so a restart never
opens a second span.

Test 3 becomes three levels deep (`run_reflex(A)` → B with engine tasks
only → C calling MCP), including a restart: C's spans nest under B under A
under the call's span.

Verdict after round 2: implement.
