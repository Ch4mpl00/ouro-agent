# Watch tasks — long-running monitoring with carried state

**Status:** design agreed (2026-09-10) — not started
**Priority:** P2
**Area:** agent / mcp / scheduler
**Created:** 2026-09-10

> **Уточнение 2026-10-16:** общий runtime процессов, carried state,
> ожидания и user input развиваются на стороне агента поверх текущего
> workflow: [durable workflows](durable-workflows.md). MCP предоставляет
> инструменты и события. Предложения ниже о владении watches/state в MCP
> и TypeScript-пути — исторические; реализацию согласовать с новым решением.

## Context

Today the agent can be told *"do X at time T"* (`schedule_task` → cron →
`scheduler` signal). It cannot be told *"watch X until it changes / until it's
over, and tell me when something happens"*.

Motivating case: **track a flight**. "Следи за рейсом PS-101, проверяй каждые
пару минут и скидывай обновления." That needs three things the scheduler has
no answer for:

1. **Carried state.** Tick N must know what tick N−1 saw, otherwise the agent
   either re-reports the same status forever or has nothing to diff against.
   `scheduled_tasks` has `cron_expr | recurring | prompt | source |
   last_run_at` and no state column; every fire starts from a blank session.
2. **A stop condition.** A flight lands. A cron doesn't. Nothing today expires
   a task except the user remembering to say "хватит".
3. **A cost model that survives a 1-minute cadence.** The current
   `scheduler` path routes through `workflow.ts` — **compile (LLM) → execute** —
   on *every* fire (the module section of `supervisor.ts`). A watch at `* * * * *` recompiles
   the same plan 1440×/day, plus a full skill preamble per session. The
   expensive half is input tokens and the per-fire compile, not the output
   tokens spent re-stating the prompt.

Point 3 is the one that decides the design. A watch is mostly **"nothing
changed"** — for a flight, ~99% of ticks. Those ticks should ideally cost
nothing at all, and must never cost a plan compile.

## Options

### O1 — recurring cron + a state document (smallest possible delta)

Keep `schedule_task` as-is. Convention only: the watch's prompt names a
unified-memory doc (`watch/flight-ps101.md`); each fire does
`read_doc` → check → compare → `append_doc`, and reports only on a delta.
Agent calls `cancel_scheduled_task` when the flight lands.

- **+** Ships today, zero schema change; unified memory already versions and
  attributes every write, and `append_doc` is the safe write op.
- **−** Two extra tool round-trips per tick, still a full compile + skill
  preamble per tick, and the stop condition lives only in the model's
  judgement. A forgotten watch runs forever — a real $-leak.
- Useful as a **stepping stone / prototype** to learn the report cadence
  before committing to schema.

### O2 — chain of one-shot tasks (the "each run schedules the next" idea)

Each fire ends by scheduling the next one-shot, passing a pointer to the
stored prompt instead of re-emitting it, so output tokens stay flat.

- **+** **Adaptive interval** — the real prize. Check every 30 min while the
  flight is 8h out, every minute during boarding/approach. Cron cannot express
  "denser as the event nears".
- **−** **The chain is not self-healing.** A crash, a failed signal, or one
  model turn that forgets the final `schedule_task` kills the watch *silently
  and permanently*. Cron re-fires regardless of what happened last tick; a
  chain has exactly one point of failure per link, and the failure mode is
  silence — the worst kind for a monitor.
- Note the adaptive interval does **not** require the chain: it needs a
  mutable interval, i.e. an `update_scheduled_task` tool (we only have
  insert/delete today). Take the prize, drop the fragility.

### O3 — first-class `watches` table + a `watch` signal source (recommended container)

Stop overloading "reminder" with "monitor". A watch is its own row:

```
watches(
  id, goal,            -- written once at creation, never regenerated
  probe,               -- what to check (see O4)
  interval_sec,        -- mutable → adaptive cadence without a chain
  state,               -- last observation, carried tick to tick
  last_report,         -- what the user was last told (dedup source of truth)
  stop_condition,      -- natural-language, evaluated per tick
  expires_at,          -- HARD stop, enforced in code, not by the model
  max_ticks, tick_count, status  -- active | stopped | expired | failed
)
```

The poller **inlines `goal` + `state` into the signal content** (exactly how
`renderContent` already inlines the cron header + prompt today). So the tick
needs *no* round-trip to fetch its own context: prompt written once at
creation, state handed over for free, output tokens flat. Tools:
`create_watch`, `update_watch`, `stop_watch`, `list_watches`. Skill:
`skills.default/watch.md`. Routing: one more `source` case, no supervisor
rewrite — the "add a domain → add a skill + a source" path CLAUDE.md already
describes.

`expires_at` / `max_ticks` are the part worth insisting on: a runaway monitor
is the failure mode that costs money, and it must not depend on an LLM
noticing.

### O4 — deterministic probe, LLM only on delta (recommended execution)

Inside that container, **split checking from reasoning**:

- At creation the agent compiles the check **once** into a `Workflow`
  (`workflow.ts` — we already have compile → execute, and `execute.ts`
  runs a plan with no LLM in the loop for tool steps). Store the compiled plan
  on the row.
- Every tick **executes** the stored plan — no compile, no skill preamble —
  normalises the result and diffs it against `state` **in code**.
- **Unchanged → write state, emit nothing, cost ≈ 0.** No signal, no session.
- **Changed → emit a `watch` signal** carrying goal + old state + new
  observation; the agent writes one human sentence and sends it.

This is what makes a 1-minute cadence sane: flight status is structured data,
the diff is mechanical, and the model is paid for only the handful of ticks
that actually say something. It also fixes the compile-per-fire cost that
today's `scheduler` path pays for *every* recurring task, watch or not.

### O5 — a durable long-running session (rejected)

Keep one agent session alive across checks, sleeping between them, so context
*is* the state. Rejected: context grows unboundedly on a multi-hour watch,
nothing survives a container restart, and the supervisor consumes one signal
at a time — a sleeping session would park the whole queue.

### O6 — push instead of poll (later, but don't preclude it)

Real flight providers push status changes over webhooks; polling is us
emulating that badly. Whatever shape lands, `probe` should be able to be
"fed externally" rather than "called on a timer", so a webhook-fed watch is a
new probe kind and not a second system.

## Decision (2026-09-10)

**O3 as the container, O4 as the execution strategy** — and O4 is generalised:
the AgentLoop gets the workflow module as a *capability*. The loop can
**author a plan, run it once to prove it works, and save it for replay**.
That makes "compile once, execute many" a property of the system, not a
special case for watches — the nightly digests recompile the same plan on
every fire today for exactly the same reason.

O2's adaptive interval is kept, as a mutable `interval_sec`, not as a
self-rescheduling chain.

### Authoring loop: compile → verify → save

New synthetic tools (`agent-loop.ts`, next to `invoke_sub_agent`):

```
compile_workflow(goal, params?)             → validated Workflow JSON (compile.ts)
run_workflow(workflow | ref, args)          → executes it, returns the bound values
save_workflow(name, workflow, params, kind) → persists it for replay
list_workflows() / read_workflow(name) / delete_workflow(name)
```

The agent's flow when the user says *"следи за рейсом PS-101"*:

1. `compile_workflow("get current status of flight PS-101")` — one planning
   call, the same compiler that serves scheduler signals.
2. `run_workflow(...)` — **the verification run**. Its value is not "it didn't
   throw": it proves the plan returns a non-empty, parseable observation, and
   the observation it returns **seeds the watch's initial `state`**, so the
   test run isn't wasted work.
3. `save_workflow("flight-status", …)` + create the watch pointing at it.
4. Every tick from then on: `executor.execute(saved plan)` — no compiler, no
   skill preamble, no ReAct loop.

### Side effects during verification — the thing that has to be right

Executing a plan fires **real** side effects. A probe that ended in
`send_telegram_message` would message the user on every verification run and
every silent tick. Two layers:

- **Probe contract (enforced).** A workflow saved with `kind: "probe"` may
  only use read-only tools and must `bind` a result; delivery steps are
  rejected at save time by `postCheckWorkflow`. Delivery is the watcher's
  job, outside the plan. This makes a silent tick provably silent.
- **Tool effect annotation (the enabler).** MCP tool registration grows an
  `effect: "read" | "write"` field, and the agent's tool registry carries it
  through. `toolsets.ts` already half-encodes this socially (`news-read` vs
  `telegram-send`); make it explicit and machine-checkable. Also unlocks a
  general dry-run mode later — stub the writes, run the reads.

### Where things live (ownership)

The executor is `packages/agent`; MCP may not import it. So:

- **The timer stays in MCP** — it already owns cron and pollers. No new MCP
  table: the agent registers a recurring `scheduled_tasks` row with
  `source: "watch"` (the column exists; `schedule_task` just doesn't expose
  it yet — one optional input).
- **The plan and the state live agent-side** in `agent.db` (Drizzle,
  migrations from code): `workflows(name, version, plan, params, kind)` and
  `watches(id, workflow_name, args, state, last_report, interval_sec,
  stop_condition, expires_at, max_ticks, tick_count, status)`. Both are agent
  domain state by the ownership rule in CLAUDE.md.
- **Routing**: the module section of `supervisor.ts` gains a `watch` case that skips *both*
  the AgentLoop and the compiler and goes straight to
  `executor.execute(storedPlan)`. Only a changed observation escalates to an
  LLM turn to phrase one sentence.

### Parameters and drift

- **Params reuse `variables.ts`.** A saved workflow declares `params`; the
  watch supplies values; the store is seeded so the plan references
  `${params.flight}`. No new substitution machinery.
- **Re-validate on load.** A saved plan names tools that can disappear (an
  upstream dropped from the gateway, a rescoped toolset).
  `createWorkflowSchema` validates against `knownTools` at *compile* time —
  a stored plan must be re-parsed at *load* time, and on failure the watch
  **degrades to a recompile** rather than dying. Same spirit as the existing
  compile-failure → AgentLoop degradation.
- **Saved plans are versioned and immutable**; a recompile writes a new
  version, so a misbehaving replay can be traced to the plan that ran.

### Why this is a small change

Nothing here is a new mechanism — `compile.ts`, `execute.ts`, `variables.ts`
and `scheduled_tasks` all already exist and are already wired together for the
scheduler path. What's new is (1) exposing compile/execute to the loop as
tools, (2) a place to persist a plan, (3) one routing case, (4) the read/write
tool annotation. The rest is the existing runner with the compiler taken out
of the hot path.

### Open questions

- `compiler.compile()` currently takes a **signal** (`{source, content,
  envContext}`), not a free-form goal. `compile_workflow` needs either a
  synthetic signal or a second entry point on the compiler surface.
- Does a saved workflow belong to a watch, or is it a shared library the agent
  can reuse across watches and digests? The second is more valuable and only
  slightly harder — it needs a name registry and the params contract above.
- Should `run_workflow` be exposed to *every* AgentLoop session, or only under
  a `watch`/authoring skill? Unrestricted, it gives the loop a way to spend
  a compile per user message.

## Cross-cutting decisions to settle

- **Delta-only vs heartbeat.** The ask was literally "каждые пару минут
  сообщалось обновление", but a message every 2 minutes saying "всё ещё в
  воздухе" is noise. Proposal: report on change, plus an optional low-rate
  heartbeat (e.g. hourly) and always on the terminal event.
- **Queue pressure.** The supervisor handles one signal at a time. A
  1-minute watch must not make Telegram replies queue behind it — another
  argument for O4, where a silent tick never reaches the queue at all.
- **Report dedup** belongs on `last_report`, not on chat history — the
  question "did I already tell them this?" should not need a Telegram fetch.
- **Discoverability.** `list_watches` so "что ты сейчас мониторишь" is
  answerable, and stopping one is a normal request.

## Acceptance

- The user can say "следи за рейсом X" in Telegram and get a watch created,
  with the interval and the stop condition echoed back once.
- State survives container restarts; a tick sees the previous observation
  without any extra tool call.
- An unchanged tick emits no Telegram message and runs no LLM session.
- A changed tick produces exactly one short Russian message.
- The watch stops on its own: stop condition met, `expires_at` reached, or
  `max_ticks` exhausted — the last two enforced in code.
- `list_watches` shows active watches; the user can stop one by asking.
- The failure mode is loud: a probe that errors N times in a row reports once
  and marks the watch `failed`, instead of dying silently.
- The agent can compile a plan, run it once, and save it — and the saved plan
  replays on later ticks **without a compiler call** (assert: zero planning
  spans in the trace of a replay tick).
- A `probe` workflow containing a write-effect tool is rejected at save time.
- A saved plan whose tool no longer exists degrades to a recompile and says so,
  instead of failing the watch.

## Notes

- Related: [[agent-working-memory]] (per-session KV — a watch needs *cross*-
  session state, which is why it lands in MCP, not in the session store),
  [[unified-memory]] (where an O1 prototype's state doc would live),
  [[centralized-config]] (interval floor / max concurrent watches).
- Compile-once-execute-many is worth extracting even if watches are deferred:
  the `scheduler` path recompiling every recurring fire is a standing cost.
- Interval floor matters. `* * * * *` against a third-party API is a good way
  to get rate-limited or blocked; the floor should be a config value, not a
  number the model picks.
