# Skills with procedures: a skill as a graph the agent drives

**Status:** pending (the general bricks `finish` and `choose` are shipped; the procedure itself is not started)
**Priority:** P2
**Area:** agent / skills, flows (OWF)
**Created:** 2026-10-19

## Context

A skill is text the model reads. Some of what a skill asks for is a fixed
procedure: Telegram reads the history, shows typing, answers once. Today
each of those costs a model turn (typing alone took a round in 35 of 39
Telegram runs), and the hard rules ("always reply", "one reply") hold only
as far as the model follows them.

Discussed 2026-10-19 (engineer/critic rounds). Rejected along the way:

- **A graph in front of the agent** (Jev or a fixed route picks "simple
  answer → llm_compose → send" vs "agent"): decides for the agent whether it
  needs tools; misroutes are silent (a "remember X" sent to a tool-less
  compose answers "Remembered ✅" and saves nothing); `start_typing` and `send`
  are Writes, so a fault after them goes to recovery, not the agent; and
  with `finish` the agent already answers a simple message in one call.
- **Jev choosing every step** (B): a classifier cannot write arguments, and
  errors compound per step.
- **`signal_handlers(source, workflow)`**: routing by source again.

## The direction (agreed)

A skill may carry a procedure: an OWF graph that **the agent owns** (writes
and revises it like a saved workflow) and that **always hands the
decisions to the agent**. Fixed steps run without a model; at a decision
point an `agent` step runs the loop with the skill's text, and the agent
leaves it through one of the step's declared exits. A `switch` follows the
exit. `choose` (shipped) is the same jump decided by Jev instead of the
agent.

```
telegram (procedure)
  1. history + start_typing        ← fixed, no model
  2. agent: skill text + history
       exits: reply(text) | done   ← the agent picks the exit
  3. reply → send_telegram_message ← fixed
     done  → end (the agent delivered itself: a question, a file, …)
```

Principle check (CLAUDE.md): the fixed steps are the agent's own earlier
decision, in an artifact it can rewrite; every decision at run time stays
with the agent; `done` keeps it free to deliver however it wants.

## Pieces

1. **`agent` step in OWF with declared exits** — a child loop as a workflow
   function; it ends by choosing an exit with arguments; a Write (needs
   `at_most_once` for model adapters, which `crates/agent/src/flows.rs` lacks) and a dry-run
   wrapper for its MCP calls.
2. **Skill ↔ procedure binding** — e.g. `procedure: <workflow>` in skill
   frontmatter; when preselection picks a skill with one, the procedure
   runs, otherwise the text as today. Chosen by meaning, not by source.
3. **A harmless-write class** — `start_typing` must not count as a started
   effect, or a fault after it skips the agent and goes to recovery.
4. **Traces** that show node, exit and the agent's loop inside it.

## Acceptance

- ouro authors a Telegram procedure itself (workflow-author) and it runs.
- Replay of past Telegram messages: the procedure matches the plain agent
  loop on outcome, with typing at no model cost and no loss of the agent's
  options (clarifying questions, several messages, files).

## Notes

- Shipped first, as general bricks: `finish` (round-level end of a
  session) and `choose` (Jev-decided jump in workflows).
- Deferred separately: prompt-cache ordering with an anchored Telegram
  history window (start from a fixed message, append-only, re-anchor past
  a budget) — Telegram-specific, lower priority (user, 2026-10-19).
