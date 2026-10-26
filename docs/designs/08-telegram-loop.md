# 08. A Telegram wrapper around agent-loop: status bubble, composed reply, slim skill

Status: implemented with the review's corrections (see Deviations); draft 2 by
the elephant, the wrapper shape is the maintainer's · Issues:
#52 (per-source loops), #53 (slim prompts; the Telegram part) · Engine:
ouro 0.3.0, no engine change · Decisions log: [LOG.md](LOG.md)

## Problem

A Telegram message runs the default path: `agent-loop` with the live
`telegram` skill (12 KB, `tools: *`), `orchestrator` and `routing`. The first
turn of "привет, как дела?" took 23.6k input tokens (trace d5f12001…,
2026-10-26). The skill carries rules the runtime no longer keeps (it says the
runtime opens a 👀 bubble; nothing does since the migration), three delivery
"hard rules" the model must remember every turn (always reply, one reply,
never a second bubble), and notes, bills, reminders and news routing that
other skills and `routing.md` own.

The maintainer wants (2026-10-26):

1. a Telegram workflow that *wraps* `agent-loop`: the loop does not know it
   runs inside a Telegram flow;
2. the skills preloaded (telegram, and what Jev preselects);
3. a 👀 bubble as soon as the message is taken, then a status line for every
   step of the loop, written by a cheap model with no context, never
   blocking the loop;
4. when the loop ends, `llm_compose` reads what happened in it and writes the
   reply;
5. the workflow sends the reply and clears the bubble.

## Decisions

1. **A built-in wrapper `telegram`** (`crates/agent/workflows/telegram.yaml`)
   with `metadata.route.when: { source: [telegram, telegram_button] }`,
   installed and activated like the other built-ins (`breaker: off`), and
   overridable by the agent like any route (design 04). It revises CLAUDE.md's
   "Telegram uses the default path unless the agent activates a matching
   loop; none is built in" (maintainer's call). It is the signal's routed
   workflow, so it has the signal's host data.
2. **The loop is a child, as `signal` runs it.** The wrapper gathers context
   (`signal_input`, `recent_activity`, `pending_confirmations`,
   `preselect_skills` with `loaded: [telegram]`), opens the bubble, then
   calls `primary_loop { input, confirm, skills: [telegram], loaded, preset:
   smart, system, exclude_tools, on_step }` and joins the child exactly as
   `signal.yaml` does (its `wait` step: no hold, the wrapper suspends until
   the child ends; a confirmation inside the loop lets it sleep durably).
   Then `signal_transcript { ref }` gives what happened. `agent-loop` is
   unchanged except decision 4. Whether `primary_loop` from a routed
   workflow is allowed (LOG: "signal routes calling `primary_loop` are
   superseded and kept") is checked by the executor; if the gate or launch
   refuses it, the smallest generic change that allows a routed workflow to
   start one primary loop is the fix, not a Telegram branch.
3. **Delivery is the wrapper's.** `primary_loop` gains `exclude_tools:
   [names]` (decision 5); the wrapper excludes `send_telegram_message`,
   `telegram_send_status`, `start_typing`, `send_telegram_chat_action`. Its
   system lines say: the reply is written and delivered after you finish;
   finish when the work is done, saying what you did and what the user
   should hear. Posting into another chat or topic goes to a worker
   (`invoke_sub_agent`), which keeps every tool.
4. **`on_step`: a generic step observer in `agent-loop`.** An optional
   input `on_step: { workflow: <name>, input: <object> }`, passed through
   `primary_loop` into the loop's input. After a `turn` whose `calls` are
   not empty, and before the guard and the round run them, the loop starts
   an owned child: `call: start_child` with `name: $context.on_step.workflow`
   and `input: $context.on_step.input + { seq: $context.i, step: {
   calls: [ {name, arguments clipped to 200 chars} ], reasoning: <the turn's
   reasoning clipped to 400> } }`, and never joins it. So the line describes
   what the model just decided to do while the calls run (the slow part).
   The engine cancels an owned child that hasn't ended when the loop ends
   (ouro `docs/api.md`: "An owned child never outlives its parent"); a
   child's failure never reaches the loop. Without `on_step` the loop is as
   before. The loop does not know who observes. The child's start can only
   fail by refusal (unknown or tripped name): that is caught in the
   `observe` step and ignored, so a broken observer never stops the loop.
5. **`exclude_tools` is generic.** Tool names removed from the loop's
   offered set after the skills' allow-lists (narrowing only), MCP and
   synthetic tools alike; carried in `LoopOptions`. A name that is no tool
   is an error at `primary_loop` (a typo must not leave the tool in).
6. **`telegram-status`, a second built-in.** Input: `id`, `chatId`,
   `messageThreadId`, `seq`, `step`. One `llm_compose { preset: base, json:
   true, prompt, input: { step } }` (the workhorse with reasoning off), the
   prompt: "one line for a Telegram progress bubble: an emoji and at most 60
   characters in Russian saying what the agent is doing now; never a result"
   → `{ "status": "…" }`; then `telegram_send_status { id, text: status,
   chatId, messageThreadId, seq }`.
7. **Bubble order is MCP's job: `seq` and a tombstone.** `telegram_send_status`
   gains an optional `seq`: an update whose `seq` is not greater than the
   last applied one for that id is dropped (`{dropped: "stale"}`); a cleared
   id (empty text) is remembered and later updates to it are dropped
   (`{dropped: "cleared"}`), so a status child cancelled mid-send can't bring
   the bubble back after the reply. Without `seq` it behaves as now, except
   after a clear. In memory with the bubbles (`tg.status`), bounded like
   them; a restart forgets (one stray bubble at worst). The full MCP instance
   owns the bubbles (the restricted one has no `telegram_send_status`).
8. **The reply is composed.** After the loop: `llm_compose { preset:
   compose, skill: telegram, json: true }` with input `request` (the
   signal's text), `history` (the recent chat lines the wrapper's
   `signal_input` gave), `work` (`signal_transcript`'s result: the loop's
   messages without the system one, as the recovery report reads them) and
   `result` (the loop's output). Prompt: write the one Telegram message that
   answers the request from what was done; never claim an action the work
   doesn't show; say plainly what failed; if nothing should be sent (a
   button press that only resolved a card), `{ "reply": null }` →
   `{ "reply": "…" }`.
9. **Sent by the wrapper.** `reply != null` → `send_telegram_message { chatId,
   messageThreadId, text: reply }`, a direct call (at most once; trusted as a
   saved workflow's call, unguarded: LOG, "calls written into a loop's YAML
   are trusted"). Then the bubble is cleared (`telegram_send_status { id,
   text: "", seq: 1000000 }`). The wrapper's output is the reply (or the
   loop's output when nothing was sent), so the trace and the activity
   journal show what the user got.
10. **Failure clears the bubble.** The loop, its join, compose and send sit
    in a `try`; the `catch` clears the bubble and raises the error again, so
    `signal`'s catch writes the recovery report as now.
11. **The slim `telegram` skill** (~2 KB, `skills.default/telegram.md`,
    `tools:` an explicit list: `get_telegram_chat_history`,
    `edit_telegram_message`, `recall`, `remember`, `get_fact`, `read_doc`):
    style (Russian unless the user writes otherwise, terse, plain text, no
    Markdown, no tables or aligned columns, `key: value` lists), topic
    discipline (pass `messageThreadId` on every call that takes it), "don't
    promise without executing", "don't invent data". Bubble, delivery,
    routing, notes, bills and reminders go: the wrapper, `routing.md` and the
    domain skills own them. Compose uses the same skill for style. The live
    overlay on prod is moved aside at deploy (Rollout).

## Non-goals

- Tool scoping by a core set and `find_tools` (#54): `orchestrator`'s
  `tools: *` stays, so the offered set is everything minus the excluded.
- Slimming `orchestrator`/`routing`/`worker` (rest of #53); a `peer`
  wrapper (#52's other half); typing indicators.
- A status line while the model is still thinking (before a turn nothing is
  known; the bubble keeps the previous line, 👀 at first).
- Known limitations (review G-M8, G-H4): the composed reply is plain text
  (no buttons), the loop never learns its reply's message id (so it cannot
  edit that reply later), and a Telegram signal's loop cannot post into
  another chat or topic (its workers inherit the deny set).
- The bubble across a long sleep (review G-M6): it is cleared when the loop
  sleeps on a confirmation card and not reopened after the press.

## Design

```
signal(telegram) → route → telegram (wrapper; the signal's routed run)
  context: signal_input, recent_activity, pending_confirmations, preselect
  bubble 👀 (seq 0)
  try:
    primary_loop(agent-loop, skills, exclude_tools, on_step) ─► agent-loop (child)
        turn → observe: start_child(telegram-status, seq i) (not joined)
             → guard → round → settle → next …
    wait (join, as signal.yaml)
    signal_transcript(ref) → work
    llm_compose(compose, skill telegram, json) → { reply }
    reply? → send_telegram_message
    clear bubble (seq 1e6)
  catch: clear bubble; raise
```

## Tests

Test-first, `crates/agent/tests/telegram_loop.rs` (new) with the fake MCP and
a scripted model, unless named.

1. A `telegram` signal runs the `telegram` wrapper (route) and an
   `agent-loop` child under it.
2. The 👀 bubble is the first MCP write, before the first model request.
3. The loop's offered tools exclude the four; an unknown name in
   `exclude_tools` fails `primary_loop`; `exclude_tools` can't add a tool.
4. `on_step`: a two-step loop starts two observer children with `seq` 1 and 2
   right after each turn and before that turn's calls run; a hanging status
   model delays nothing (the reply arrives; the hanging child is cancelled
   when the loop ends); without `on_step` no child starts; an observer name
   that doesn't exist is ignored and the loop completes.
5. Compose gets `request`, `history`, `work` (no system message) and
   `result`; its reply is sent once to the conversation's chat and thread;
   then the bubble is cleared; the wrapper's output is the reply.
6. `{ "reply": null }` sends nothing and clears the bubble.
7. A loop fault clears the bubble and the recovery report runs.
8. A crash after the send and before the clear: no second send; the bubble
   is cleared on resume.
9. A guarded write waiting for a confirmation: the wrapper sleeps durably
   with the loop; after the press the loop continues and the reply is
   composed and sent.
10. MCP (`crates/mcp`, unit): `seq` drops a stale update; a cleared id drops
    later ones; no `seq` behaves as before except after a clear.
11. The slim skill: `shipped_skills.rs` validates its tool list; under 3 KB.

Hand mutants (once, scratch copy): the observer joined (test 4); the
tombstone removed (test 10); `exclude_tools` ignored (test 3).

## Rollout

Deploy, then move `/var/lib/ouro/skills/telegram.md` on prod to
`/var/lib/ouro/telegram.md.pre-08` (outside the skills directory) so the
shipped skill applies; skills are read per signal, nothing restarts. Watch
the first Telegram traces: the first turn's input tokens, the bubble lines,
the composed reply, the latency added by compose.

## Open questions

- Compose model: `compose` (gpt-6.1-sol) by default; `smart` if its latency
  shows.

## Deviations

Written by the executor before changing course: the zero-context review
(goldfish, 5 high / 11 medium / 7 low) is applied as corrections, with the
maintainer's decisions on the high ones. Each entry names the review item.

- **review G-H1: `start_loop`, not `primary_loop`.** A route runs with loop
  host data, so the dispatcher's `primary_loop`, `settle_child` and
  `signal_transcript` refuse it, and the gate refuses a route that calls
  them. Three generic host functions (context `agent-loop`, so an agent's
  copy of `telegram` passes the gate) replace them in the wrapper:
  `start_loop { input, loop?, skills, loaded, preset, system, exclude_tools,
  on_step }` starts one owned loop child (kind `primary`) whose
  conversation, confirmation target, session, env, signal id and guard
  evidence come from the caller's host data, never from YAML, and whose
  instructions start with the caller's own (the session context, the
  signal source, its env addendum, a handover, a fallback line, a button
  note); `loop_outcome { ref }` answers `{status: completed | faulted |
  cancelled, output, error}`; `loop_transcript { ref }` (G-M3). The
  dispatcher's functions stay dispatcher-only; `primary_loop` also takes
  `exclude_tools` and `on_step`.
- **maintainer, on G-H1: `start_loop` starts any named loop.** Its `loop`
  argument (default `agent-loop`) names the loop it runs, a future
  `research-loop` the agent writes included; the start goes through
  `runs::start_named_child` at the loop site, so overrides and fallbacks
  (design 04) apply. Any run in a signal's tree with loop host data may
  call it, an agent-saved workflow included (the gate allows it). The
  child's host data is the caller's, only narrowed: preset, skills,
  `exclude_tools` (added to the caller's own), `on_step` and the system
  lines may be chosen; the conversation, the confirmation target and the
  session cannot be redirected.
  TODO(ouro#43): once `run: workflow`/`start_child` can inherit host data,
  `start_loop` (and `signal`'s `primary_loop`) give way to `run: workflow`
  and are removed.
- **review G-H2: `observe_step`, a host function, not YAML `start_child`.**
  ouro 0.3.0's reserved `start_child` turns an unknown, tripped or paused
  name into an interruption, which would stall the loop (an engine bug the
  maintainer files in ouro). The loop's `observe` step calls `observe_step {
  i, content, calls }`; `on_step` lives in the loop's host data; the start
  goes through `runs::start_named_child` (a new site, `Observer`, with
  nothing in place of a set-aside observer), and any refusal answers `{id:
  null}`. `step.reasoning` does not exist (G-L1): the observer gets the
  turn's `content` (often empty) and its calls. The step sits after
  `route`'s `act` branch, and is skipped for rounds of only `finish` /
  `working_memory_*`.
- **review G-H3: `exclude_tools` is a deny set in host data.** `LoopOptions`
  and `LoopHost` carry it (serde default: stored runs still read); `allows()`
  and `visible()` check it, never `allowed`, so a restart and a `tools: *`
  skill loaded mid-loop keep it. A name that is no tool (the MCP snapshot,
  MCP's current list, the synthetic tools; G-L3) fails the start.
- **review G-H4: nobody but the wrapper sends the reply.** Workers inherit
  their loop's deny set (a narrowing is inherited, as the confirmation
  target is), so decision 3's "posting into another chat goes to a worker"
  is dropped: the Telegram loop cannot post elsewhere (Non-goals).
  `orchestrator.md`/`routing.md` lose "you own delivery" for a conditional
  line; the wrapper's system lines say the reply is written and sent after
  the loop finishes and must not be sent or delegated; compose answers
  `{reply: null}` when the work shows the reply already delivered.
- **review G-H5: the composed reply is guarded.** The wrapper sends it as
  the loop does: `start_guard` on the one `send_telegram_message` call, the
  join, then `classify`/`tool_once` under the guard run (the route's run has
  loop host data, so those functions serve it). The deterministic rules and
  Jev apply; a block or a declined card clears the bubble and the wrapper's
  output is the guard's reply.
- **review G-M1:** 👀 is the wrapper's first step, before any model call.
- **review G-M2:** every status call (open, clear, the catch's clear) sits
  in a `try` with an empty catch; the send's `call.unknown` counts as sent
  (no resend, no report); any other send error raises into the catch.
- **review G-M3:** `loop_transcript` drops the system message and renders
  the turns compactly (calls with clipped arguments, results clipped per
  call), keeping the request and as much of the tail as fits.
- **review G-M4:** `start_loop`'s span is tagged `activity: loop`, so the
  journal's `final` is the route's (the reply sent); `settle_child { kind:
  route }` takes the skills of a route that took no turn from its
  `start_loop` record (`loaded`).
- **review G-M5:** `signal_transcript` follows a route that took no turn to
  its loop child, so the recovery report sees the loop's work.
- **review G-M6:** the bubble id is `status:<signal id>`. When the loop
  sleeps on a confirmation the wrapper clears the bubble (it would freeze
  above the card for hours); the tombstone then keeps later status lines
  away, and nothing reopens it.
- **review G-M7:** `launch` adds the undelivered button press's note to
  every route's instructions (which `start_loop` inherits). A press that
  resolves a card never reaches the wrapper; the null-reply example is "a
  press the agent decides needs no answer".
- **review G-M8:** replies with buttons and the reply's message id inside
  the loop: Non-goals.
- **review G-M9:** each observer is anchored under the loop's span as
  `observe:<i>` (`activity: observer`); the journal skips what is under it.
- **review G-M10:** the built-in route has `priority: -1`, so an agent's
  route of equal specificity wins without a tie; the default-path tests that
  send `telegram` signals are ported (see the LOG entry).
- **review G-M11:** the observer's compose has `timeout: PT20S`, so it never
  holds the supervisor's quiescence for long.
- **review G-L2, decision 7 narrowed:** `seq` and the tombstone gate only
  updates that carry a `seq`; an update without one behaves exactly as
  before, after a clear too (an agent or a saved workflow reusing an id
  after clearing it keeps working, and the existing bubble property holds).
  Marks are kept in the per-id queue's turn and dropped after an hour; the
  answer is `{action: "dropped", reason: "stale" | "cleared"}`.
- **review G-L4:** compose gets `history: $context.input` (the rendered
  history and the request), no separate `request`.
- **review G-L5:** accepted: `llm_compose` always prepends `composer`.
- **review G-L6/G-L7:** rollout also moves a `telegram.patch.md` aside and
  checks for an agent route on `source: telegram`; unjoined observers are
  stored runs the sweep removes like any other.
