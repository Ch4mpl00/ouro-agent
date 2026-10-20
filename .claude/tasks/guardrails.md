# Guardrails on the agent loop's writes

**Status:** planned
**Priority:** P1
**Area:** agent / owf / decisions
**Created:** 2026-10-20

## Context

The agent acts through writes (Telegram replies, peer replies, memory and
document writes, schedules, skills, workflows). Tool data it reads (mail,
news, channel posts, web pages) can carry instructions; a model can also
overreach, pick the wrong target, leak data or repeat a write whose outcome
was unknown. Decided 2026-10-20 (with the user): a guard step in the agent
loop, decisions by Jev, enforced by the host, blocking from day one.

## Design

- **A step in the loop.** `agent-loop.yaml` gets a `guard` step between
  `turn` and `round`: it passes the round's calls and gets a verdict per
  call. Reads skip the guard; all writes of a round go to Jev in one
  decisions request (one `choice` question per call).
- **Enforced by the host, not by the YAML.** The guard's policy (the Jev
  question, the reasons, the deterministic rules) lives in git where the
  agent cannot write it (`write_skill` refuses its name; it is never an
  argument from YAML). `tool` sends a write only with an `allow` verdict
  the host recorded for that call; with none (a loop without the step, a
  restart between `guard` and `tool`) `tool` runs the guard itself. So an
  override of `agent-loop` cannot drop or weaken it.
- **Refused early.** `save_workflow`/`activate_workflow` refuse an
  `agent-loop` version with no `call: guard` before `call: tool`, with a
  clear message. The host check above is the real guarantee.
- **The runtime informs, the agent decides.** A blocked call is not sent;
  the model gets the reply `[guard] blocked: <reason> — <guidance>` like a
  tool error, and decides what to do.
- **Guard input:** the user's request (the signal's first user message),
  the call (tool + arguments), the writes already made in this session.

### Reasons (Jev chooses one per write)

| Label | When |
| --- | --- |
| `allow` | the action serves the user's request |
| `off_request` | not asked for; a side effect beyond the request |
| `injected` | dictated by tool data (mail, news, post, page), not by the user |
| `wrong_target` | right action, wrong recipient / chat / thread / document |
| `data_leak` | secrets, tokens, IBANs, personal data sent where they do not belong |
| `destructive` | irreversible or overwriting without an explicit request |
| `repeat` | repeats a write already made in this session |
| `needs_confirmation` | plausible but high-impact: ask the user first |

Deterministic layer before Jev (no model): `denied_tool`, `unknown_chat`
(`send_telegram_message` to a chat that is not known), `secret_pattern`
(token / key / IBAN patterns in outgoing text). If Jev is unavailable:
fail-open for the reply to the current signal's own chat, fail-closed
otherwise, with `guard_unavailable` in the reply.

Each label carries the model-facing guidance text that follows it in the
reply.

## TODO

- `needs_confirmation` / `destructive` are blocks for now. Real
  confirmation: the run waits durably for the user's answer (an event wait,
  which OWF `listen` / the interpreter does not support yet) and then sends
  or drops the call.
- A golden set of real writes and injections (like `news-filter-eval`) to
  measure false blocks and misses; judge the guard's verdicts.
- Outgoing text checks beyond patterns (tone, a reply to the wrong person).

## Acceptance

- Every write of every loop has a recorded verdict; none is sent without
  `allow`, including under an overridden `agent-loop`.
- A blocked call comes back to the model with its reason and guidance.
- An `agent-loop` version without the guard step is refused on save and
  activation.
- A round of reads costs no guard call; a round of writes costs one
  decisions request.
