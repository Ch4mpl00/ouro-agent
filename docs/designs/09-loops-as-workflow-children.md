# 09. Loops as workflow children (ouro 0.4 host inheritance)

Status: v2, after the maintainer's decision of 2026-10-27 (LOG: an
activated workflow is trusted code); implemented, with the deviations in
LOG "2026-10-27 (design 09: executor)" · Issues: ouro#43 (done in ouro 0.4.0)

v1 kept a trust boundary between YAML and the runtime; its Codex Sol review
(five high findings, all at that boundary) led the maintainer to drop it
instead of adding an admission hook to the engine. CLAUDE.md, "An activated
workflow is trusted code", states the model this design follows.

## Problem

A workflow can start a child three ways: the engine's `run: workflow` and
`start_child`, and the agent's `start_loop`, which exists only because engine
children had no host data. ouro 0.4.0 copies the parent's host data to a child
on `host: inherit`. One way remains for every YAML author, the maintainer and
the agent: `call: start_child { name, host: inherit, input }`, joined with
`listen`. `run: workflow` stays denied in the agent's versions, because the
engine resolves its version, one the agent never named.

## Scope

- **In:**
  - the engine dependency moves to ouro v0.4.0;
  - `start_loop` is removed;
  - a loop started with `start_child` + `host: inherit` takes its parameters
    from its input;
  - `telegram.yaml` uses it, and so can any wrapper the agent writes;
  - `loop_outcome` and `loop_transcript` take the ref `start_child` returns.
- **Out:**
  - `primary_loop`, `start_worker`, `start_guard`, `recovery_loop` and
    `observe_step` stay as host functions, the built-ins' plumbing;
  - moving the Telegram wrapper out of the shipped built-ins;
  - YAML macros.

## Decisions

1. **Host data and input.**
   - **Host data is inherited byte-equal.** An inherited loop child's host
     data is its parent's `LoopHost`, unchanged. From it the child takes the
     tree it belongs to: the conversation, the confirmation target, the
     session (so it shares the same working memory), the signal, the env,
     the guard's root request, the inherited system parts, the iteration
     limit, `parent_id`, the tool whitelist and the deny set.
   - **The request comes in the input.** The child's input is
     `{ request: { input, loop?, skills, loaded, preset, system,
     exclude_tools, on_step, allow_skill_writes } }`. These are the
     arguments `start_loop` took.
   - **Checks are for mistakes, not authority:**
     - the preset must be selectable;
     - each skill must exist;
     - each `exclude_tools` name must be known.

     `allow_skill_writes` is honoured as asked. The deny set is the
     parent's plus `exclude_tools`, and workers inherit it as now.
   - **A bad request faults the child.** It is a child fault, not a refusal
     at the start. The caller joins the child and reads `loop_outcome`
     (failed, with the message), as it would for any other failure.

2. **Preparation, done once.**
   - **Where it happens.** The first host function that runs for such a run
     (any of them, through `loop_for`) prepares it. Nothing in the YAML
     marks it, so no step can skip it.
   - **What it builds.** The options, built the way `start_loop` built them:
     the parent's `signal_input` and `preselect_skills` records (read
     through `RunReads` on the ancestor), the skill text (`prepare`), the
     guard's evidence (the loop's own skills block, its earlier messages,
     its first user message), the first `messages` and the memory
     reference.
   - **Insert-once.** The result is stored insert-once, keyed by run id, in
     a new agent.db table `prepared_loops` (one migration; swept with the
     run's evidence). The first stored preparation wins: a crash before or
     after storing it recomputes or reuses it, and once stored, every later
     call, recovery and guard reads it. Nothing reads the skill files or the
     request again.
   - **The guard uses it too.** `loop_for` and `guard_context_of` build
     from (inherited host data + stored preparation), so the guard judges
     against the child's skills, not the parent's.
   - **Recognition.** An inherited child is a run admitted by a workflow
     (not a host child) whose input has `request`. A run admitted by a host
     function keeps today's path.

3. **Tracing.**
   - **Anchoring.** Preparation anchors the child's trace (`run_trace`,
     insert-or-get by run id) under the parent run's current span, as
     `start_loop` did (`agent_loop`, activity `loop`). A redispatch finds
     the anchor and opens no second span.
   - **Skills shown for a wrapper.** `settle_child` and the activity reader
     take a wrapper's executed skills from the child's stored preparation,
     instead of from `start_loop` records.

4. **No hidden fallback.**
   - **A refusal is the wrapper's to handle.** A tripped, paused or
     inadmissible child version makes `start_child` refuse; since ouro 0.4
     (#42) the refusal is catchable. The wrapper's `catch` decides what to
     do.
   - **No substitution.** The runtime substitutes nothing for this path,
     so an operator's pause holds. `start_named_child`'s fallback stays for
     the host functions that use it.
   - **Built-in Telegram wrapper.** `telegram.yaml` catches a refused start
     and answers the user that the loop is unavailable, through the same
     compose-and-send tail, with the refusal's message in the transcript it
     composes from.

5. **The gate.**
   - **Still denied:** `run: workflow`, `schedule`, `emit` and the breaker
     policy.
   - **Allowed:** `start_child` with `host: inherit`. Its argument-name
     check must accept `host`.

## Tests (test-first)

1. **Telegram cases pass on the new start.** `telegram.yaml` on
   `start_child` + `host: inherit` passes the existing `telegram_loop.rs`
   cases, ported from `start_loop`.
2. **A custom loop round-trips.** A `research-loop` started from an agent's
   wrapper by `start_child`: its outcome and transcript are read back (the
   design 08 test). The wrapper passes the gate and a dry run.
3. **Restarts don't change the loop.** A restart before preparation is
   stored, and again in the middle of the loop, with the skill file changed
   in between. The loop keeps the first stored preparation (same system
   prompt, skills, exclusions), and the guard's question names the child's
   skills.
4. **A bad request faults the child.** An unknown preset or a missing skill:
   the child faults with the reason; the wrapper's `loop_outcome` reads it.
5. **Tracing.** The child's trace nests under the parent's span, and a
   redispatch adds no span.
6. **A paused child version is refused.** The wrapper's catch runs, and no
   other version starts.

Hand mutants:
- preparation recomputed on every `loop_for` (test 3);
- the deny set taken from the request alone, without the parent's (test 1's
  exclusion case).

## Rollout

The supervisor and the engine run in one process, so there are no mixed
workers. Before deploying, query the prod engine store for live runs whose
records show a `start_loop` call. Today that can only be a Telegram signal
sleeping on a confirmation. Cancel such runs or let them finish. Then deploy,
with `start_loop` removed in the same release. Active agent versions calling
`start_loop` would stop passing the gate's registry check; prod has none
(checked 2026-10-27: only built-ins).
