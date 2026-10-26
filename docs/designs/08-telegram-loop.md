# 08. A Telegram wrapper around agent-loop: status bubble, composed reply, slim skill

Status: draft 2 (elephant; the wrapper shape is the maintainer's) · Issues:
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
