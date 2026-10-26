# 08. A Telegram loop: status bubbles, a composed reply, a slim skill

Status: draft (elephant) · Issues: #52 (per-source loops), #53 (slim
prompts; this does the Telegram part) · Engine: ouro 0.3.0, no engine change
· Decisions log: [LOG.md](LOG.md)

## Problem

A Telegram message runs the default path: `agent-loop` with the live
`telegram` skill (12 KB, `tools: *`), `orchestrator` and `routing`. The
first turn of "привет, как дела?" took 23.6k input tokens (trace
d5f12001…, 2026-10-26). The skill carries rules the runtime no longer keeps
(it says the runtime opens a 👀 bubble; nothing does since the migration),
three "hard rules" about delivery (always reply, one reply, never a second
bubble) that the model must remember on every turn, and notes, bills,
reminders and news routing that other skills and `routing.md` own.

The maintainer wants the channel's flow in YAML (2026-10-26):

1. preload the skills (telegram, and what Jev preselects);
2. a 👀 bubble the moment the message is taken, then a short status line on
   every step of the agent, written by a cheap model with no context,
   without blocking the loop;
3. the agent loop;
4. last, `llm_compose` sees the user's message and what the agent did and
   writes the reply;
5. the reply is sent automatically; the bubble is cleared.

## Decisions

1. **A built-in `telegram-loop`** (`crates/agent/workflows/telegram-loop.yaml`)
   with `metadata.route.when: { source: [telegram, telegram_button] }`,
   installed and activated like the other built-ins (`breaker: off`). The
   agent may override it like any loop (design 04). This revises CLAUDE.md's
   "Telegram uses the default path unless the agent activates a matching
   loop; none is built in" (maintainer's call). It is route-only: workers
   keep `agent-loop`; a worker naming `telegram-loop` gets the existing
   refusal for routed workflows, if any, else runs it without a signal and
   skips the context, status and delivery steps (`$context.signal == null`).
2. **Delivery is the workflow's, not the model's.** The model never sends
   the reply: `loop_setup` gains an optional `exclude_tools: [names]`
   (decision 6), and the loop excludes `send_telegram_message`,
   `telegram_send_status`, `start_typing` and `send_telegram_chat_action`.
   Its system lines say: the reply is written and delivered after you
   finish; finish when the work is done, saying what you did and what the
   user should hear. Posting into *another* chat or topic is delegated to a
   worker (`invoke_sub_agent`), which keeps every tool, as domain work is
   today.
3. **The 👀 bubble at once.** The first step after the context (before
   `loop_setup`, so before any model call) is a direct
   `telegram_send_status { id: "status:<signal id>", text: "👀",
   chatId, messageThreadId, seq: 0 }`.
4. **A status line per step, non-blocking.** After each `turn` that has
   calls, the loop starts an owned child of a second built-in,
   `telegram-status`, with `call: start_child` and does not join it. Its
   input: the bubble id, chat, thread, `seq` (the turn index `i`), and a
   digest of the step: the calls' names with their arguments clipped to
   200 chars and the turn's reasoning clipped to 400. The child runs one
   `llm_compose` with preset `base` (the workhorse with reasoning off),
   `json: true`, prompt: "one status line for a Telegram progress bubble:
   an emoji and at most 60 characters in the user's language saying what
   the agent is doing now; never the result" → `{ "status": "…" }`, then
   `telegram_send_status` with that text and `seq`. A failure of the child
   is its own: it never reaches the loop (owned children end with the
   parent; nobody joins it). An unfinished child is cancelled when the loop
   ends.
5. **Bubble order is MCP's job: `seq` and a tombstone.** Status children
   race (step 3's line can land before step 2's). `telegram_send_status`
   gains an optional `seq` (integer): an update whose `seq` is not greater
   than the last applied one for that id is dropped and answered
   `{dropped: "stale"}`; a cleared id (empty text) is remembered and every
   later update to it is dropped (`{dropped: "cleared"}`), so a slow status
   child can't bring a bubble back after the reply. Without `seq` the tool
   behaves as now (but a cleared id still drops). Kept in memory with the
   bubbles themselves (`tg.status`), bounded like them; a restart forgets,
   which costs at most one stray bubble.
6. **`loop_setup.exclude_tools` is generic.** A list of tool names removed
   from the loop's offered set after the skills' allow-lists are applied
   (narrowing only: it can't add a tool). It names nothing about Telegram;
   the YAML does. It applies to MCP tools and synthetic tools alike. A name
   that is not a tool is an error at `loop_setup` (a typo must not silently
   leave the tool in).
7. **The reply is composed.** After the loop ends (the model answered
   without calls, or `finish`), one `llm_compose`:
   - `preset: compose` (the text model), `skill: telegram` (its style rules
     ride along as the system part), `json: true`;
   - input: `request` (the signal's text), `history` (the preloaded recent
     chat lines the loop got, as given to `loop_setup`), `work` (the loop's
     transcript without the system message: each assistant turn's text and
     calls, each tool reply clipped to 1 500 chars, the last 30 entries),
     `result` (the model's last text or the `finish` result);
   - prompt: write the one Telegram message that answers the request from
     what was done; never claim an action the work does not show; if a tool
     failed, say so plainly; if nothing should be sent (a button press that
     only resolved a card), answer `{ "reply": null }` → `{ "reply": "…" }`.
8. **Sent by the workflow.** `reply != null` → `send_telegram_message
   { chatId, messageThreadId, text: reply }` as a direct call (at most
   once, trusted as a saved workflow's call, unguarded: LOG, "Calls written
   into a loop's YAML are trusted"). Then the bubble is cleared
   (`telegram_send_status { id, text: "", seq: 1000000 }`). The run's
   output is the reply text (or the `result` when nothing was sent), so the
   signal trace and the activity journal show what the user got.
9. **Failure clears the bubble.** The loop and the compose step sit in a
   `try`; the `catch` clears the bubble and raises the error again, so the
   `signal` workflow's catch writes the recovery report as now.
10. **The slim `telegram` skill** (~2 KB, `skills.default/telegram.md`,
    `tools:` an explicit list: `get_telegram_chat_history`,
    `edit_telegram_message` and the memory core — `recall`, `remember`,
    `get_fact`, `read_doc`): style (Russian unless the user writes
    otherwise, terse, plain text, no Markdown, no tables or aligned
    columns, `key: value` lists), topic discipline (pass `messageThreadId`
    on every call that takes it), and "don't promise without executing" /
    "don't invent data". Everything about the bubble, delivery, routing,
    notes, bills and reminders goes (the workflow, `routing.md` and the
    domain skills own them). The live overlay on prod
    (`/var/lib/ouro/skills/telegram.md`) is moved aside at deploy so the
    shipped one applies; the old text is kept in the backup only.
11. **Context steps as the reference loop.** `signal_input`,
    `recent_activity`, `pending_confirmations` (when `confirm`),
    `preselect_skills` (`loaded: [telegram]`), `loop_setup { skills:
    [telegram], exclude_tools, input, system }` — as
    `tests/fixtures/telegram-loop.yaml` does, which this replaces as the
    reference (the fixture stays for the tests that use it, or those tests
    move to the built-in).

## Non-goals

- Tool scoping by a core set and `find_tools` (#54); `orchestrator`'s
  `tools: *` stays, so the offered set is everything minus the excluded.
- Slimming `orchestrator`/`routing`/`worker` (rest of #53).
- Typing indicators (`start_typing`): the bubble replaces them here.
- A loop for `peer` (#52's other half).

## Design

```
signal(telegram) → route → telegram-loop
  context: signal_input, recent_activity, pending_confirmations, preselect
  bubble 👀 (seq 0)
  loop_setup (skills, exclude_tools)
  try:
    turn → [calls?] → start_child telegram-status(seq i) (not joined)
         → guard → round → settle → next … (as agent-loop)
    compose (llm_compose, json) → { reply }
    reply? → send_telegram_message
    clear bubble (seq 1e6)
  catch: clear bubble; raise
```

`telegram-status`:

```yaml
do:
  - line:
      call: llm_compose
      with: { preset: base, json: true, prompt: "…", input: { step: '${ .step }' } }
  - show:
      call: telegram_send_status
      with: { id: '${ $input.id }', text: '${ .status }', chatId: …, messageThreadId: …, seq: '${ $input.seq }' }
```

The guarded round, `classify`/`tool_once`/`tool_repeat`, worker joins and
`settle` are copied from `agent-loop.yaml` as the fixture does ("keep in
sync"); a test compares the round's steps of both files so they can't drift.

## Tests

Test-first, in `crates/agent/tests/telegram_loop.rs` (new) with the fake MCP
and a scripted model, unless named.

1. A `telegram` signal runs `telegram-loop` (route), not `agent-loop`.
2. The 👀 bubble is the first MCP write, before the first model request.
3. The model's offered tools exclude the four; `loop_setup` with an unknown
   name in `exclude_tools` fails; `exclude_tools` can't add a tool (unit).
4. A two-step run starts two `telegram-status` children with `seq` 1 and 2;
   the loop's next turn does not wait for them (a status model that hangs
   doesn't delay the reply); each child sends `telegram_send_status` with
   its `seq`.
5. Compose gets `request`, `history`, `work` (no system message, clipped)
   and `result`; its reply is sent once with the conversation's chat and
   thread; then the bubble is cleared; the run's output is the reply.
6. `{ "reply": null }` sends nothing and clears the bubble.
7. A loop fault clears the bubble and the signal's recovery report runs.
8. A crash after the send and before the clear: the send is not repeated
   (at most once); the bubble is cleared on resume.
9. MCP (`crates/mcp`, unit or spec): `seq` drops a stale update; a cleared id
   drops later updates; no `seq` behaves as before except after a clear.
10. The guarded round of `telegram-loop.yaml` equals `agent-loop.yaml`'s.
11. The slim skill: `shipped_skills.rs` validates its tool list against the
    server; its size is under 3 KB.

Hand mutants (once, scratch copy): the status child joined (test 4); the
tombstone removed (test 9); `exclude_tools` ignored (test 3).

## Rollout

Deploy, then on prod move `/var/lib/ouro/skills/telegram.md` to
`/var/lib/ouro/telegram.md.pre-08` (outside the skills dir), restart nothing
else (skills are read per signal). Watch the first Telegram traces: input
tokens of the first turn, the bubble's lines, the composed reply.

## Open questions

- The compose model: `compose` (gpt-6.1-sol) by default; `smart` if its
  latency shows.
