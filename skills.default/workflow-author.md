---
tools: []
---

# Authoring saved workflows

Author and change workflows for the agent's own loops and for scheduled
tasks. Use for requests to change the agent cycle, action order, exit rules
or recurring work.

The runtime exposes its own control flow as editable Open Workflow
Specification definitions. You can author:

- **Loops** (`document.metadata.loop`): how model turns, guarded tool rounds
  and workers proceed, and when a cycle ends. A loop that declares
  `metadata.route` handles the matching signals by itself: it gathers its
  own context, sets up its instructions and runs. A channel's or a source's
  flow is one such loop. Start from `read_workflow(name: "agent-loop")`.
- **A dispatcher override** named `signal`: only when dispatch itself must
  change (rare).
- **Scheduled pipelines**: fixed steps run when their scheduled task fires;
  model adapters such as `llm_compose` and `filter_relevant` provide judgement.

The user can describe the desired behavior without naming workflows or OWF.
"Fix the Telegram cycle", "show a status before the first model turn", and
"use another cycle for peer signals" are workflow-authoring requests.
Skills provide model instructions; explicit workflow steps and switches
establish execution order and exit conditions. Loops and dispatcher
overrides need no cron task. For a runtime change follow the procedure and
"Loops"; the scheduled-pipeline advice applies to scheduled work.

## Procedure

1. Identify whether the request needs a scheduled pipeline, a loop or (rarely)
   a dispatcher override. For runtime work, pin down which signals match, the
   required sequence, exit conditions and behavior on errors. For a
   requested time today, compare it with the current local time. If it
   already passed, ask what time/date to use; never silently move it to
   tomorrow.
   Pin down the ask: what the user wants to receive or have done, from
   which data, how often, and what counts as new or relevant. Ask the
   parent back only when a choice would change what the user gets.
2. Find what you will build on. `workflow_functions` lists every function
   a workflow can call; `workflow_functions(name)` shows one in full. The
   user's ask is the whole specification: every rule they gave (what to
   take, what to drop, the order, the format, the tone) goes into the
   definition itself, word for word where it matters. Inspect existing saved
   definitions with `list_workflows` and `read_workflow`; for a loop, read
   `agent-loop` and preserve its host contracts. A loop may load relevant
   transport/domain skills with `loop_setup`. For scheduled pipelines, put
   the user's rules in the definition and use `llm_compose`'s `skill` only
   for one the user names.
3. Call the data-reading functions the workflow will use with `probe_function`,
   with the arguments the workflow will pass, and build each expression from
   the fields you saw in the reply. Do not skip this: `save_workflow`
   checks argument names, not the fields of replies, so a wrong field is
   just `null` at run time and the workflow quietly does the wrong thing on
   every run. The reply is the only truth about names, units and meanings:
   never fill them in from what you know of the service behind a function.
   A probe shows the shape of the data, not the data a run will see: never
   write the values it happened to return (texts, names, ids, amounts)
   into the definition; judge what matters by criteria (`filter_relevant`) or
   by fields, never by the sample's words. Runtime host functions such as
   `agent_turn` and `loop_setup` need an admitted run; inspect their use in
   the base definitions and test them through a signal dry run instead.
4. Design it with the principles below, write the definition and call
   `save_workflow(name, definition)`. It checks the definition against the
   language, the functions and their argument names. Fix every reported
   problem and save again until it is accepted; a `warnings` list in the
   accepted reply names flows that are legal but usually a mistake: fix
   them too unless you meant exactly that.
5. Try it: for a scheduled pipeline, `try_workflow(name)` runs the saved draft
   on today's data as a scheduled fire would, with writes recorded; one that
   takes agent turns, on a fire of its task (below). For a
   loop or `signal`, use `try_workflow` with `signal` and every changed draft,
   as described below. A completed run of the wrong loop does not test your
   change.
   Read the report as the user would live with it:
   - it ended `completed`, and its `path` is the one you meant: every
     task that ran, in order; a task of another branch in it (the "nothing
     new" text after the real one) means a task falls through into the
     next;
   - every value came through: a `null` or an empty string in a step's
     output or a write's arguments, where data should be, is a wrong
     field or a wrong expression;
   - the message in `writes` meets every requirement the user stated:
     go through them one by one (order, format, merging, what to leave
     out, language) against the sample, and treat each miss as a defect;
     beyond those: complete, readable, nothing made up, nothing they
     already got;
   - the bookkeeping writes come after the delivery, with the values the
     next run needs: after a read with a limit, the watermark is the last
     item read, not `env.now`.
   A defect you see is yours to fix, not to report, and so is anything
   you would call questionable (a noisy selection, a split event): sharpen the criteria,
   the threshold or the prompt, save a new version and try again; three
   tries at most, since each one costs a run's model steps. Tell the user
   only what needs their decision, or what three tries could not fix. A
   quiet day shows only the "nothing new" path: say so rather than invent
   data to test with.
6. Return to the parent, in Russian, a description the user can check
   without reading a definition, in plain words:
   - what they get and what triggers it (matching signals or scheduled times
     on their clock); for runtime work, which signals the loop takes and what
     steps or exit rules change;
   - where it comes from, named the way the user knows it, not by
     function names;
   - how it picks what matters, what it does when there is nothing new,
     and what it changes besides sending (what it remembers between
     runs);
   - the dry run's loop, action order and completion rule for runtime work,
     and whether every intended draft actually ran;
   - the message the dry run would have sent, quoted as it is, as a
     sample ("Вот что пришло бы сейчас: …"), or that today was
     quiet;
   - anything the user should know or decide (a source that was empty,
     a choice you made for them).
   No definition, YAML, JSON, function names or task paths. End with one
   line for the parent listing every workflow's name, kind and tried version,
   and the cron only for scheduled work. Do not activate or schedule: the parent activates
   after the user confirms, and schedules only work that needs a timer.

To change a workflow: `read_workflow`, edit, `save_workflow` (a new
version), and the same dry run and confirmation. Schedules name the workflow, not a
version, so they run whatever version is active.

## Designing scheduled pipelines

A workflow is a fixed pipeline; its model steps are the only places that
judge. Build the pipeline the task needs from these parts, no more:

- **Sources.** The read functions that hold the data, scoped to what is
  new since the previous run. Prefer a function's own filters (a time
  bound, a category, a limit) to fetching everything and filtering later.
  Read independent sources in one `fork`.
- **Window.** A run covers the time since the previous one: a lookback
  that matches the schedule, or a watermark. A read with a limit can stop
  before the window's end; then the next run must start where this one
  stopped: advance the watermark to the last item read (its own time
  field), not to `env.now`, or what was past the limit is never read.
  Size the limit for the longest gap between runs, not for the dry run's
  window, which is shorter. Avoid calendar-day arithmetic
  in jq: `env.now` is UTC, and the user's day is not.
- **Dates the user reads** (a header, "today", "this morning") come from
  `env.localNow`, the same instant on the user's clock with its offset;
  pass it to the model step that writes them. `env.now` (UTC) is for
  function arguments.
- **State between runs.** The input carries two watermarks,
  `env.newsLastReadAt` and `env.techLastReadAt`, stamped with `set_memory`
  under `news_digest.last_read_at` / `tech_digest.last_read_at`; any other
  key `set_memory` writes cannot be read back by a workflow. For anything
  else, keep a journal stream named after the workflow: `journal_append`
  what you delivered (the real text, never a "nothing new" note),
  `journal_tail` it at the start, and let the model steps leave out what
  it already covers.
- **Selection.** Many items of which some matter → `filter_relevant` with
  the criteria spelled out (what to take, what to drop). It judges each
  item on its own, so it cannot tell that two items report the same
  thing: merging and leaving out what was already sent belong to the
  `llm_compose` that writes the text. Items an expression can judge (a
  field, a threshold, a date) → jq, no model.
- **Computation.** Sums, counts, filters, sorting and grouping are jq
  (`[.rows[] | select(.value < 0) | .value] | add`), never a model step: a
  model adds cost and can miscount. Hand the model the computed numbers and
  let it only write the text.
- **Branching on a judgement.** A path that depends on something an
  expression cannot test (is this worth sending, which kind of request is
  it) → `choose` with labelled options, then a `switch` on `.choice`. Never
  branch on a model's free text, such as an exact sentinel phrase from
  `llm_compose`: a changed letter or a full stop sends the wrong branch.
  When the step that writes the text is also the one that knows (it
  deduplicates, it counts what is left), give it `json: true`, spell out
  the object in its prompt (`{"quiet": true}` or `{"quiet": false, "text":
  …}`) and branch on a field (`.quiet == true`).
- **Synthesis.** Text for a person → `llm_compose`. A model step sees its
  prompt and its `input`, nothing else: not this conversation, not the
  user's messages, not other steps. Write the user's rules for the text
  into its prompt in full, and pass the data in `input`. Leave `preset`
  out for what the user reads (the default `compose`, the writing model);
  `base` for mechanical reshaping.
- **Delivery.** `send_telegram_message` to
  `$workflow.input.env.telegramChatId` unless the user named another chat;
  never write a chat id into the definition.
- **Bookkeeping last.** Stamp a watermark or append to a journal only after
  the delivery it stands for succeeded, so a failure cannot mark unsent
  data as handled. Data a run read and judged not worth sending is handled
  too: advance the watermark on that path as well, or the next run reads
  and judges it again.
- **Nothing new.** Decide it explicitly: skip the delivery with `if`, or
  send a short note. For a frequent schedule a note on every quiet run is
  noise; say which you chose.
- **Few model steps.** Every `llm_*` call costs time and money on every
  run.
- **Schedule.** One task, all its times in one cron (`0 9,19 * * *`), on
  the user's clock. Name the workflow after what it does, lowercase with
  dashes.

## Runs and failures

Once a run starts, its definition and input are pinned. Restart reuses saved
step results; `wait` and retry delays release the worker and resume from
persisted deadlines. A run that fails before any write falls back to the
agent; one that fails after a write is only reported.

Every version has a breaker, the runtime's: three faults of its runs in a
row set it aside until a version is activated again (activating the same
version again clears it too). Your versions cannot declare one of their own
(`metadata.ouro.breaker` is refused). A set-aside version never runs: the
built-in runs in its place, a route gives way to the next route or the
default path, a scheduled task's workflow to the agent loop, and whatever
runs instead is told why in one line of its instructions. An operator may
pause a name: it falls back the same way, and you cannot activate it until
they resume it.

A call of an MCP tool the server does not mark read-only is delivered at
most once: a lost answer is an unknown outcome, never sent again. A read is
delivered at least once: a lost answer can repeat it. To hold any call to at
most once, set `metadata: { ouro: { delivery: at_most_once } }` on its
`call` task. An unknown outcome then stops the run without retry; execution
can be lost if the agent crashes before dispatch. Put this metadata on
external calls inside reusable functions, not on their callers.
`llm_compose`, `filter_relevant` and `choose` support only the default
policy. Do not wrap unknown outcomes in retries.

A `try` retry does not repeat a write that already succeeded: the retry
reuses its saved result and goes on, while a write that failed is sent
again. If the retried attempt would send different arguments (text a model
composed again inside the `try`), the run fails with a conflict and sends
nothing, so keep each `try` narrow, around the call it retries, with the
`llm_compose` before it. Reads and model steps run again on every attempt.

## Tasks with agent turns

Any workflow may take agent turns, not only loops: every run the runtime
starts on a signal is admitted with the model's parameters (a scheduled
task's: the `base` preset and the `scheduler` skill;
`metadata.agent: { preset, confirm }` changes them). A recurring task that
needs judgement starts as a copy of `agent-loop` under its own name:
`read_workflow("agent-loop")`, rename `document.name`, `save_workflow`,
dry-run it on a fire of its task (`try_workflow(name, signal: {source:
scheduler, content: <the task's prompt>, payload: {workflow: <name>}})`),
activate it once the user agrees, then `schedule_task(workflow: <name>)`.
The task's prompt is the first user message. Later versions move what
repeats into deterministic steps around the turns (a `for` calling a read
directly, its results given to the model through `loop_setup`), leaving the
model the judgement and the errors. Wherever `agent_turn` appears, keep
agent-loop's guarded round (`turn`, `start_guard`, the `round` with
`classify` → `tool_once` | `tool_repeat`, `settle`): save and activation
refuse a workflow without it. Drop the built-in's `metadata.ouro.breaker`
from your copy. Three faults in a row hand the task to the agent loop until
a new version is activated.

## Loops

Every agent loop, the parent's and each worker's, runs a named loop
workflow; the built-in is `agent-loop`. Saving and activating a version
named `agent-loop` changes every loop's default (the recovery report's too);
activating the built-in's current version again undoes it. A differently
named loop runs when its route matches a signal, or when a worker selects
it (`invoke_sub_agent(loop: …)`, or a worker skill's frontmatter `loop:`).

**A channel's or a source's flow is ONE loop that declares its route.**
Never write a separate signal route for it, never copy `signal` or its
recovery `catch`: the dispatcher already offers button presses to waiting
runs, opens the session, runs a scheduled workflow, picks the route and
reports a failure. Start from `read_workflow("agent-loop")` and change the
minimum:

1. Rename `document.name`, drop `metadata.ouro` (the breaker policy is the
   built-in's alone) and add `metadata.loop.description` and
   `metadata.route.when` (below). Keep the guarded round as it is: `turn`,
   `start_guard`, the `round` (worker joins, `classify` → `tool_once` |
   `tool_repeat`), `settle`.
2. Before the first turn, gather context with the steps you need, under one
   `if: '${ $context.signal != null }'`: a worker running the same loop has
   no signal and skips them (the route's input carries the signal). `signal_input` (the message with its source's
   history), `recent_activity` (the activity journal), `pending_confirmations`
   (`target:`, cards already waiting in the conversation) and
   `preselect_skills` (`request:`, `context:`, `loaded:`).
3. `loop_setup` (once, before the first `agent_turn`; later it is refused)
   puts them into the instructions: `system` (strings; nulls are dropped),
   `skills` (loaded in full, with their tools; they count for the guard) and
   `input` (replaces the first user message, the signal's text). Export its
   `messages` and `memory` into `$context`.
4. Fixed steps of the loop (a status bubble before the first turn and its
   cleanup) are direct `call:` steps, trusted as any saved workflow's calls
   once the user approved the definition. `tool_once` and `tool_repeat` are
   only for the model's calls: they carry the guard's verdict.
5. Exit rules decide on `agent_settle`'s `outcomes` (below).

```yaml
document:
  dsl: '1.0.4'
  namespace: agent
  name: chat-loop
  version: '1.0.0'
  metadata:
    loop: { description: Telegram conversations; ends on a reply to the chat. }
    route: { when: { source: [telegram, telegram_button] } }
input:
  from: '${ . + { i: 0 } }'
do:
  - context:
      if: '${ $context.signal != null }'
      do:
        - input: { call: signal_input, export: { as: '${ $context + { input: .input } }' } }
        - activity: { call: recent_activity, export: { as: '${ $context + { activity: . } }' } }
        - setup:
            call: loop_setup
            with:
              skills: [telegram]
              input: '${ $context.input }'
              system: '${ [ $context.activity, "Delivery: reply with send_telegram_message to the current chat." ] }'
            export: { as: '${ $context + { messages: .messages, memory: .memory } }' }
        - status:
            if: '${ $context.conversation != null }'
            call: telegram_send_status
            with: { id: '${ "status:" + ($context.signal.id | tostring) }', text: 👀, chatId: '${ $context.conversation.chatId }' }
  - turn:
      call: agent_turn
      with: { messages: '${ $context.messages }', memory: '${ $context.memory }', i: '${ $context.i }' }
      export: { as: '${ $context + { messages: ($context.messages + [ .message ]), calls: .calls, i: ($context.i + 1) } }' }
  - route:
      switch:
        - answered: { when: '${ .calls == [] }', then: clear }
        - act: { then: guard }
  - guard:
      call: start_guard
      with: { calls: '${ $context.calls }', i: '${ $context.i }' }
      export: { as: '${ $context + { guard_run: .run, guarded: (.guarded // []) } }' }
  # round: as in agent-loop, unchanged (workers, classify, tool_once / tool_repeat)
  - settle:
      call: agent_settle
      with: { calls: '${ $context.calls }', results: '${ . }', memory: '${ $context.memory }' }
      export: { as: '${ $context + { messages: ($context.messages + .replies), memory: .memory } }' }
  - next:
      switch:
        - replied:
            when: '${ $context.conversation as $c | $c != null and any(.outcomes[]; .ok and .name == "send_telegram_message" and (.arguments.chatId | tostring) == ($c.chatId | tostring)) }'
            then: clear
        - finished: { when: '${ .finished }', then: clear }
        - more: { then: turn }
  - clear:
      if: '${ $context.signal != null and $context.conversation != null }'
      call: telegram_send_status
      with: { id: '${ "status:" + ($context.signal.id | tostring) }', text: '', chatId: '${ $context.conversation.chatId }' }
```

**Route.** `when` matches `source` and top-level signal payload fields;
values are strings or nonempty string lists, and all keys must match. More
keys win; for overlapping routes with equally many keys, larger `priority`
wins (default 0); activation refuses ties and names the conflicting
workflow. A routed loop starts on the `smart` preset with the signal's
conversation (else the default chat) as its confirmation target;
`metadata.route.preset` (`base`, `smart`, `smartest`) and
`metadata.route.confirm: none` (or the same under `metadata.agent`) change that. Its instructions start with the
session context, the signal's source and its env addendum; its input carries
`signal` (`id`, `source`, `content`, `env_context`, `payload`, `button`,
`created_at`) besides `messages`, `memory`, `env`, `conversation` and
`confirm`. Telegram uses the default path until a matching loop is active.
A route set aside (its breaker, an operator's pause, a tool it calls gone)
gives way to the next matching route, then to the default path, which is
told why. A workflow that calls the dispatcher's own functions
(`primary_loop`, `route_signal`, …) is a `signal` override: it declares no
route.

**Contracts.** The model's parameters (preset, skills, confirmation target)
are the host's, fixed when the run is admitted; YAML can neither read nor
set them. The input tells the loop's own steps `conversation` (`{chatId,
messageThreadId}` of the incoming conversation; absent for workers) and
`confirm` (where confirmations go). Every round must start the guard child
(`start_guard`) before running the model's calls; save and activation check
it. `agent_turn` takes
`extra_turn: true` to allow one turn past the iteration limit (a single
reminder; keep its flag in context so it fires once). `agent_settle`
returns `finished`, `output`, `replies`, `memory` and ordered `outcomes`,
one per call: `{name, arguments, ok, result}` (`ok` is false for failed,
blocked or unknown outcomes; `result` capped at 64 Ki UTF-16 units). Decide
on what a call asked for (`arguments`) and whether it succeeded (`ok`),
never on a write's reply: a dry run answers every write with a mock. Compare
chat ids as strings. Select the matching outcome's `arguments.text` as the
output if desired.

**Workers.** `invoke_sub_agent` accepts `loop:`; otherwise the first worker
skill with frontmatter `loop: <name>` supplies it, else `agent-loop`; its
description lists the active loops. A loop's `start_worker` can select one
with `loop:`, which overrides those preferences. A name that is no loop, or
is set aside, runs `agent-loop` instead. Workers inherit the confirmation
target. Give `start_worker`, `tool_once` and `tool_repeat` the call's place
(`i`, `index`) so owned workers, joined with `listen` (`metadata: { ouro: {
hold: PT15M } }`), survive restarts.

**Resolution and safety.** A name runs its active version, pinned when its
run starts. Each version has its own breaker (above): set aside, a loop
gives way to the built-in `agent-loop` and `signal` to the built-in
dispatcher, for that run only; your version stays active and shows as
tripped in `list_workflows`. A loop or a `signal` override is activated like
any workflow: after a completed dry run on a signal and the user's
agreement. The guard child sequences review, host-rendered cards and
confirmation listens (60 second hold, then a durable sleep); Rust verifies
consumed button events against exact calls, so YAML cannot authorize a
model's call. The built-in `signal` routes `telegram_button` presses through
`deliver_event` first; its catch reports failures through `recovery_loop`
(skipped on cancellation and aborts).

## Trying a loop or the dispatcher

A loop and `signal` run on signals, so you try them on one:
`try_workflow(name, signal: {source, content, envContext?, payload?},
drafts: [...], confirm?)` runs the signal as if it had just arrived, through
the whole chain: the dispatcher, the loop its route picks, the guard,
workers. `name` and every name in `drafts` run at their latest saved version
instead of the active one, in this run only: `try_workflow(name: "signal",
signal: …, drafts: ["chat-loop"])`. Also try a signal outside the loop's
route to check that it still takes the default path.

- **The signal.** Model it on a real one; never invent its shape.
  `try_workflow(name: "signal", signal: {})` succeeds without a draft or a
  run: `status: "signal_types"`, with the types and latest real examples in
  `signal_types`. Copy a matching example's form (a
  Telegram signal's header line with the chat and thread, a peer's
  "Message from …, thread N"), change only the text. A source that is not
  one of them is refused.
- **What is real.** Model turns (they cost what a real signal costs), reads,
  the internet and MCP's reads included, and the guard (its rules and its
  model). Every write is recorded instead, with its arguments, and answers
  `{"mock": true, "note": "dry run: not executed", "delivered": true}`,
  marked `mock` in the trace; a worker runs, dry too. Nothing is kept: no
  message, card, memory, journal entry, schedule, skill or workflow changes.
- **Confirmations.** A call the guard asks the user about is answered on the
  spot: `confirm: "accept"` (the default), `"decline"` or `"expire"`; try
  each branch your loop must handle.
- **The trace.** `status` and `output`, `ended` (how the primary loop ended:
  `finish`, `answer`, a `workflow rule` and the step it ended at, `limit` or
  `fault`; `nudged` when it got a reminder), `writes` (in the order they
  happened, each with its `function`, `arguments`, the `run` and `task` it
  came from, and `via`: `yaml` for a workflow's own call, `tool` for a
  model's, `handler` for one the runtime made itself), and `runs`: each
  workflow that ran, with its `version` (`draft vN` for yours), its `key`
  (how its parent started it: `route`, `primary`, `guard`, `worker:i.j`),
  its `path` (every step in order, `task → case` for each switch, a fault
  with its error; a jq error names the expression and the value it met), a
  loop's `turns` (reasoning, text, calls with their `verdict`,
  `confirmation`, `ok` and `result`), a `fallback` line when a version was
  set aside for it, and `children` (guard rounds, workers). A signal that
  faulted shows its failure report's own dry run under `recovery` (its
  runs and writes). A large reply is in your working memory (`memory_key`)
  with a preview. A `sandbox_refusals` count is the runtime's fault, not
  your workflow's: report it.
- **Judging it.** Compare the trace with what the user asked for, as for a
  scheduled workflow: the loop taken, each turn, each call's arguments,
  how the loop ended and what it answered. Fix and try again, three tries
  at most. `drafts` with `ran: false` did not run and stay untried
  (`outcome` says why: `did not run`, or `set aside: …` with the reason):
  check the signal against the loop's route `when`.

A completed try marks every draft whose runs all completed as tried;
activation needs it.

## The language

`listen: { to: { one: { with: { type: <literal or expression> } } } }`
consumes one matching event. Use `any: [ { with: { type: ... } }, ... ]`
instead of `one` to accept the first event of several types. Output is an
array of `{ type, data, time }`. Durable runs suspend without holding a
worker; `timeout: { after: PT24H }` raises the standard catchable timeout.
Dry runs fail on an empty listen. `all`, `until`, correlation, read and
foreach listen options are unsupported.

A `listen` task may set `metadata: { ouro: { hold: PT5S } }` (a literal ISO 8601
duration, default `PT0S`). It first consumes an available event, then waits
in memory for host notifications during that hold, keeping its lease and
heartbeat. Delivery continues the run without saving a waiting state. When
the hold or the task timeout ends, it suspends durably; recovery replays the
listen and consumes the persisted event or handles the timeout. A crash during
the hold is recovered through the ordinary lease and journal.

`start_child { name, input?, host? }` starts the active version of a
workflow immediately as an owned durable child and returns `{ id, key, done,
waiting }`. The invocation path identifies it across replay; its admitted
definition and input stay immutable. Without `host` it gets no host data, so
it cannot take agent turns: only its own steps run. Listen on its `done` /
`waiting` event types (use an in-memory hold for fast work). After `waiting`,
listen on `done` without a hold to release the worker. `child_outcome { id,
key }` reads the owned child's stored terminal outcome; event data alone is
not authority. Ending a parent cancels unfinished children.

To run a loop (`agent-loop` or one of yours) from a workflow that has a
signal's context (a route, a scheduled task's workflow), start it with
`host: inherit` and its parameters in `input.request`:
`start_child { name: agent-loop, host: inherit, input: { request: { input,
skills, loaded, preset, system, exclude_tools, on_step, allow_skill_writes }
} }`. Only `input` (the first user message) is required. The loop keeps
your run's conversation, confirmation target, session and excluded tools;
its instructions are your run's, then `system`; `exclude_tools` takes more
tools from it and its workers. A bad request (an unknown preset, skill or
tool name) faults the loop; join it with listen, then read `loop_outcome {
ref }` (completed with output, faulted with error, cancelled) and
`loop_transcript { ref }` (what it did), `ref` being what `start_child`
returned. A paused or tripped loop refuses the start with an error you can
catch; nothing runs in its place, so the catch decides what the user hears.

The built-in `guard` workflow is reserved and readable, but cannot be saved
or activated; its review and confirmation functions are private. An
`agent-loop` override must call `start_guard` before its tool round, and keep
the `begin` step (`loop_begin` when `$context.messages` is null) if a
workflow may start it.


A subset of the Open Workflow Specification 1.0, in YAML, with the
runtime's extensions under `metadata.ouro`. Anything not listed here is
rejected (`emit`, `run`, `schedule` and `metadata.ouro.at` — schedule_task
starts work on time —, `metadata.ouro.breaker`, catalogs, secrets,
authentication).

```
document: { dsl: '1.0.4', namespace: agent, name: <name>, version: '1.0.0' }
use:      { functions: { <name>: <task> } }     optional; `call: <name>` runs it
timeout:  { after: <duration> }                 optional, the whole run
do:       [ - <task name>: <task>, … ]          names unique in their list
```

A task is a map with exactly one of these keys:

| key | form | output |
|---|---|---|
| `call` | `call: <function>`, `with: { arg: value }` (an object) | the reply |
| `set` | `set: { key: value, … }` or `set: '${ … }'` | that value |
| `do` | `do: [ tasks ]` | the last task's |
| `for` | `for: { each: x, in: '${ … }', at: i }`, `do: [ tasks ]`, `while: '${ … }'`? | the last pass's: each pass gets the previous pass's output, so collect per item with `export` |
| `fork` | `fork: { branches: [ tasks ], compete: false }` | the branches' outputs in order (`compete: true`: the first to finish) |
| `switch` | `switch: [ - <case>: { when: '${ … }', then: <flow> } ]` | its input; the first true case's `then` decides, a case without `when` is the default |
| `try` | `try: [ tasks ]`, `catch: { errors: { with: { status: 503 } }?, as: e?, when?, exceptWhen?, retry: { delay: PT5S, backoff: { exponential: {} }, limit: { attempt: { count: 3 } } }?, do: [ tasks ]?, then: <flow>? }` | the body's, or the handler's |
| `raise` | `raise: { error: { type: <uri>, status: 400, title?, detail? } }` | — (fails the run unless caught) |
| `wait` | `wait: PT10M` or `wait: { minutes: 10 }` | its input |
| `listen` | `listen: { to: { one: { with: { type: event } } } }` (or `any: [filters]`) | array containing the consumed event |

And any of: `if: '${ … }'` (false → skipped, its input passes on),
`input: { from: '${ … }' }` (what the task sees), `output: { as: '${ … }' }`
(what it returns), `export: { as: '${ … }' }` (the new `$context`),
`timeout: { after: <duration> }`, `then: <flow>`, on a `call`
`metadata: { ouro: { delivery: at_most_once } }`, and on a `for`
`metadata: { ouro: { concurrency: 4 } }` (its passes at once, at most that
many).

A `<flow>` is `continue`, `exit` (this list), `end` (the run), or the name
of a task in the same list — never a task body: to branch into work, name a
task. `continue` runs the next task in the list, whatever it is, so a task
reached only by a jump (an error to raise, an alternative message) goes
after the main path, and the main path's last task ends with `then: end`.
Durations are ISO 8601 (`PT30S`, `P1D`) or `{ days, hours, minutes,
seconds, milliseconds }`.

Expressions are jq inside `${ … }`, and a value is either text or one whole
expression: `"Привет ${ .name }"` is an error, write
`'${ "Привет " + .name }'`. In an expression: `.` (the task's input),
`$context`, `$workflow.input`, `$task`, a `for`'s `each`/`at` names, a
`catch`'s `as` name. jq's `now`, `env` and `input` are withheld: time comes
from `$workflow.input.env.now`.

YAML: quote a value that starts with `${` in single quotes; inside single
quotes a `'` is written `''`. Write long text (a prompt) as a block scalar,
which needs no escaping:

```
prompt: |
  Text over several lines, quotes ' and " as they are.
```

Data flow: each task's output is the next task's input. A scheduled task's
workflow gets `{ env: { timezone, now, localNow, newsLastReadAt,
techLastReadAt, userEmail, telegramChatId }, signal: { id, source, content,
env_context, payload, created_at }, messages, memory, confirm }` (`messages`:
its instructions and the task's prompt, for a workflow that takes agent
turns), available everywhere as `$workflow.input`. Keep a result for later tasks with
`export: { as: '${ $context + { name: . } }' }` and read `$context.name`.

Functions besides the ones `workflow_functions` lists from the tools:

- `llm_compose { skill?, prompt?, input?, preset?, json? }` — one generation
  (with `skill`'s instructions when one is named); `input` is an object
  shown to the model as tagged sections; `preset` is `compose` (default,
  the writing model), `base`, `smart` or `smartest`. Returns the reply, parsed when it is JSON. With
  `json: true` the model must answer one JSON object (describe its fields
  in the prompt); any other reply faults the step instead of flowing on as
  text.
- `filter_relevant { items, prompt, threshold? }` — keeps the items of the
  array that pass the rules in `prompt`. Not a chat model: a classifier
  answers "passes / does not pass" for every item separately (in batches
  of 25), with a probability; an item is kept from `threshold` up (0.3 by
  default, higher keeps fewer). Write the prompt as rules an item either
  passes or fails; it sees one item at a time, never the others.
- `choose { question, options, input?, min_confidence?, default? }` — a
  jump the classifier decides: `options` maps 2–255 labels (lowercase
  letters, digits, `_`) to the criterion of each, and Jev picks one judging
  `input` (up to 24 000 characters as JSON). Returns `{ choice, confidence,
  probabilities, fallback }`. With `min_confidence` and `default` (one of the
  options, given together), a less confident answer becomes `default` with
  `fallback: true`. A classifier failure faults the run, never a silent
  default: catch it with `try` when a branch is safe to take without it.
  Follow it with a `switch` on `.choice` whose `then` targets are fixed;
  saving warns when a case is missing for an option (and there is no
  default case) or compares with a label that is not one. A dry run makes
  the real choice, so it shows one branch: tell the user which.

  ```yaml
  document: { dsl: '1.0.4', namespace: agent, name: example, version: '1.0.0' }
  do:
    - gather:
        call: search_news
        with: { query: rust }
        export: { as: '${ $context + { items: .items } }' }
    - worth_sending:
        call: choose
        with:
          question: Is there anything in input.items that is worth a digest?
          options: { digest: Something new and significant, quiet: Nothing new or nothing significant }
          input: { items: '${ $context.items }' }
          min_confidence: 0.6
          default: digest
    - route:
        switch:
          - quiet: { when: '${ .choice == "quiet" }', then: end }
          - digest: { then: compose }
    - compose:
        call: llm_compose
        with: { prompt: Write the digest., input: { items: '${ $context.items }' } }
  ```
- `set_memory { key, value }` — stamps one of the two watermarks above.
- `hash_value { value }` — SHA-256 of canonical JSON (object key order does
  not matter); returns `{ hash }`. No model call or writes.
- `skill_catalog_snapshot {}` — returns `{ skills: [{ name, content,
  content_hash, routable, tools }] }` from the agent's active skill store.
  Content includes live/default resolution and improver patches. The hash
  covers effective content and execution metadata; `routable` defaults to
  true and can be disabled in skill frontmatter. No model calls or writes.

The installed `skill-index` workflow maintains a JSON document at
`agent-system/skill-index.md` using the memory tools. Its prompt and jq
pipeline live in its definition and can be edited through ordinary workflow
versions. `write_doc(searchable: false)` keeps service documents out of
recall and embeddings; later writes and patches preserve that setting when
it is omitted. Memory tools can return `{ ok: false, error: ... }` as a normal
reply: always check `ok` before treating a write as successful.
## Reference

Every construct of the table in one definition. Its function names stand
for real ones (`read_records`, `read_sent`, `remember_sent`); it shows the
shape of each construct, not a pipeline to copy.

```yaml
document: { dsl: '1.0.4', namespace: agent, name: example, version: '1.0.0' }
do:
  - gather:
      fork:
        branches:
          - records:
              try:
                - read: { call: read_records, with: { since: '${ $workflow.input.env.now }' } }
              catch:
                errors: { with: { status: 503 } }
                retry: { delay: PT30S, backoff: { exponential: {} }, limit: { attempt: { count: 3 } } }
          - sent: { call: read_sent, with: { n: 5 } }
      output: { as: '${ { records: .[0].items, sent: [.[1].entries[].body] } }' }
      export: { as: '${ . }' }
  - totals:
      set:
        count: '${ .records | length }'
        sum: '${ [.records[] | .value] | add // 0 }'
      export: { as: '${ $context + . }' }
  - lines:
      for: { each: record, in: '${ $context.records }', at: i }
      do:
        - line:
            set: '${ $context.lines // [] }'
            export: { as: '${ $context + { lines: (($context.lines // []) + [$record.title]) } }' }
  - route:
      switch:
        - quiet: { when: '${ $context.count == 0 }', then: end }
        - busy: { then: continue }
  - write:
      call: llm_compose
      with:
        prompt: |
          Summarise the records for the user, in Russian. Leave out what
          input.sent already reported; nothing left → an empty reply.
        input: { lines: '${ $context.lines }', sum: '${ $context.sum }', sent: '${ $context.sent }' }
      export: { as: '${ $context + { text: . } }' }
  - send:
      if: '${ $context.text != "" }'
      call: send_telegram_message
      with: { chatId: '${ $workflow.input.env.telegramChatId }', text: '${ $context.text }' }
  - remember:
      if: '${ $context.text != "" }'
      call: remember_sent
      with: { body: '${ $context.text }' }
```
