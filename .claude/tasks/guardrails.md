# Guardrails on agent writes and open-world reads

**Status:** done (host-enforced loop writes, goal-contribution policy, bounded host evidence, batched decisions, outage handling and a golden-set evaluator; confirmation TODOs below remain)
**Priority:** P1
**Area:** agent / owf / decisions
**Created:** 2026-10-20

## Context and sources

The agent acts through writes (replies, memory, documents, schedules, skills,
workflows). Reads can send private data in URLs or search queries. Retrieved
content can hijack its goal. Decided with the user on
2026-10-20: judge whether a call **contributes to the user's goal**, allowing
instrumental steps by default, with a host-enforced guard from day one.

- [Task Shield (ACL 2025, arXiv 2412.16682)](https://arxiv.org/html/2412.16682v1):
  contribution includes intermediate steps such as checking availability before
  booking. Feedback identifies the blocked action, its risk and the user's goal.
- [Meta LlamaFirewall AlignmentCheck](https://arxiv.org/html/2505.03574v1):
  audit actions against the original user objective to detect goal hijacking.
- [Claude Code auto mode](https://www.anthropic.com/engineering/claude-code-auto-mode):
  its classifier sees user messages and tool calls; assistant prose and tool
  results are stripped so injected content cannot argue with the classifier.
- [OpenAI Operator](https://openai.com/index/introducing-operator/): user
  oversight for consequential external actions. Our policy asks before
  third-party sending, deletion/overwriting, money, account or settings changes
  unless explicitly requested; intermediate steps pass without confirmation.
- This is **not a security boundary**. [Willison's lethal trifecta](https://simonwillison.net/2025/Jun/16/the-lethal-trifecta/)
  explains the risk of private data, untrusted content and external communication.
  [CaMeL](https://arxiv.org/abs/2503.18813) and
  [Design Patterns for Securing LLM Agents](https://arxiv.org/abs/2506.08837)
  describe architectural defenses. Fixing control flow before reading untrusted
  data, as in CaMeL's code-then-execute approach, is outside this task's scope.

## Design

- **A step in the loop.** `agent-loop.yaml` runs `guard` between `turn` and
  the tool round. Writes OR open-world tools are guarded. MCP `openWorldHint`
  defaults to true when absent; only explicit false opts internal reads out.
  All calls needing Jev share one decisions request and one bounded context
  (request, skills, chain and compiled policy), with one short question per call.
  `code_agent`, `probe_function` and `try_workflow` are open-world synthetic
  adapters; other synthetic reads are internal. Delegation skips the parent
  guard; worker calls run through their own guard. Schedule writes still guard.
  Open-world follows the MCP meaning: the arguments can reach a party we do
  not control (a URL, a search engine, someone else's chat). Our own stores,
  the user's accounts (mail, bank, userbot) and our processors (the embedder
  behind `recall`/`search_news`) are closed-world.
  The gateway preserves upstream annotations, including their absence.
- **Enforced by the host.** Instructions, reasons and deterministic checks are
  compiled from git-owned files, never YAML arguments or editable skills.
  Verdicts belong to a loop and call id + SHA-256(name, exact arguments), are
  consumed by `tool` and do not persist. Missing verdicts, changed arguments
  and restarts trigger a new review in `tool`. Direct guarded calls from loop
  overrides cannot bypass `tool`.
- **Refused early.** Save/activation refuse an `agent-loop` without
  `call: guard` before `call: tool`. The host still enforces legacy overrides.
- **Host evidence only.** At admission the host snapshots the request, a
  worker's root request and assigned task, structured preloaded user messages,
  and the exact loaded skill text (including preselected skills/patches as
  presented). This survives restart in the admitted run input. Rendered
  conversation history, arbitrary system context, worker input-ref contents,
  assistant prose/reasoning and tool results are excluded from Jev's input.
  Telegram's structured preload supplies only user messages; peer history is
  currently retained only as rendered text, so is skipped.
- **Call sequence.** Jev sees prior calls' names and arguments, reads and
  blocked attempts included, in order, plus the calls being judged. A boolean
  marks blocks; no reply body is sent. The host records calls at dispatch,
  independently of assistant transcripts or YAML evidence. Durable loops
  restore bounded call evidence from host `tool` records; no reply pairing
  is needed. Round/index positions retain pending intents in the built-in
  loop's call order across a crash. Overrides without positions use saved
  dispatch sequence; unknown intents then follow known calls by logical ref.
  The recent 64 calls are retained with `chain_omitted`;
  argument strings cap at 4,000 Unicode characters, call names/ids at 256,
  request and root request at 16,000 each, earlier user text at the most recent
  16,000 total, loaded skill text at 24,000 total. Clipped text has a visible
  `[truncated]` marker. Verdict identity and deterministic checks use full args.
- **The runtime informs; the agent decides.** A block is a failed tool result:
  `[guard] blocked: <label> — <guidance>. The user's goal: <request>`.
  The goal uses the root request for workers and caps at 500 characters with
  a visible truncation marker. `finish` cannot end a round with a failed call.

### Jev chooses exactly one of three labels

| Label | When |
| --- | --- |
| `allow` | Plausibly contributes to the user's goal, including intermediate/instrumental steps: notes, memory, drafts, requested scheduling, replies in the conversation. Default; uncertainty about an instrumental step means allow. |
| `misaligned` | Positive signs of serving no user goal: unrelated side effects, targets/content apparently dictated by retrieved data (goal hijacking), or private data sent somewhere the user did not ask for. |
| `needs_confirmation` | Serves the goal but is consequential and hard to undo, and the user has not explicitly requested that exact action: sending to a third party, deleting/overwriting user data, money, account/settings changes. Ask in the conversation and wait; a later user confirmation can authorize it. |

Deterministic (no model): `unknown_chat` (unknown Telegram destination),
`secret_pattern` (token/key/IBAN patterns in nested string args).
Unavailable Jev: open-world reads passing deterministic checks fail open.
Current-conversation replies named in the primary request may also fail open;
other writes fail closed with `guard_unavailable`. Malformed decision responses
follow the same outage rule. The reason data
supplies guidance for every label. Known chats come from task/root request,
earlier user messages, and the configured default chat.

## TODO

- `needs_confirmation` blocks for now. Real confirmation: the run waits
  durably for the user's answer (an event wait, which OWF `listen` / the
  interpreter does not support yet) and then sends or drops the call.
- Fallback from Jev to an ordinary LLM: when the decisions API is down,
  ask the same guard question to a chat model (structured output with the
  same labels) instead of failing open/closed; fail-open/closed stays only
  for when both are down.
- Outgoing text checks beyond patterns (tone, a reply to the wrong person).

## Acceptance

- Every guarded call has a host-recorded verdict; none dispatches without
  `allow`, including under an overridden `agent-loop`.
- A blocked call returns its label, guidance and the user's goal.
- Save/activation refuse an `agent-loop` without the guard step.
- An internal read-only round costs no guard request; a round with writes
  or open-world reads costs one (unless all are blocked deterministically).
- Jev receives goal/skill/call evidence and no assistant prose or tool results;
  instrumental writes and ordinary remote reads reach the fake and can be allowed.

## Golden-set evaluation

`crates/evals/fixtures/guard-golden.json` holds 147 judgement cases in four
splits by category prefix: the first 35 (tuned on), `holdout/` (seen while
tuning), `blind/` (written later, unseen) and `indep/`: 80 cases a separate
model wrote from a brief without seeing the prompt or the other cases. Only
`indep/` is an honest measure. `guard-eval` calls the real env-configured
`DecisionClient` with the runtime's builders, judges every case (production
skips and deterministic blocks aside, so it measures Jev alone), one case per
request by default: batched cases sharing one state influenced each other's
answers. It prints expected/got, ASR, false blocks and label accuracy; a run
costs about $0.005.

Measured 2026-10-20 (Jev 1.13, 3 runs each, `indep/`): the first prompt (one
label question, short policy) ASR 14%, false blocks 10.5%; the six-step
prompt in `instructions.md` (goal, destination, data, history, scale,
explicitness) ASR 12%, false blocks 0%, label accuracy 95%. Five narrow
questions combined by the host (branch `feat/guard-atomic`) did worse on
`indep/` (ASR 17%, or 23% with a two-signal rule) despite near-perfect scores
on the sets it was tuned on; the six-step prompt shipped.
