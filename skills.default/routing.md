---
routable: false
tools: []
---

# Skill routing for the primary agent

Choose a domain skill for each focused piece of work. The source name is a
hint, while the user's actual request determines the task. Transport skills
(`telegram`, `peer`, `scheduler`) are loaded on the parent; domain skills are loaded
by `invoke_sub_agent`. Use `list_skills` to discover an unfamiliar domain.
Do not `read_skill` just to delegate: the child loads its named skills.

Typical routes:

- News only when the user explicitly asks for news ("новости", "дайджест
  новостей", "что нового в мире / в IT"). A status question ("как дела",
  "доложите обстановку", "что у тебя") is about the agent and its tasks:
  answer it from the activity journal and pending work, never with a news
  sweep. A full news sweep has no skill: a general worker reads the channel
  posts and follows the user's rules from the chat; one the user wants
  regularly is a saved workflow (`workflow-author`).
- `news-query`: a specific news subject, region, person or event.
- `tech-digest`: IT news across HN, Habr and Telegram, when asked for.
- `nashdom-bill`: utility mail and PDF extraction.
- `dreaming`: skill maintenance, run only when the user asks for it (or
  schedules it). It may update skills as assigned; it should report what
  changed.
- `workflow-author`: author or change a saved task, the agent's own loop,
  signal handling or routing. Examples: "присылай каждое утро …",
  "поправь цикл для Telegram", "сначала покажи статус", "заканчивай после
  ответа", or "для peer используй другой цикл". Workflows include scheduled
  pipelines, named `agent-loop` variants and signal routes selecting those
  loops by source or payload. A schedule is needed only for scheduled tasks.
  Delegate these requests even if discovery did not select the skill; a skill
  edit alone cannot establish a required execution order or exit condition.
  The worker saves drafts, tries the scheduled pipeline on today's data or
  the route/loop chain on a signal, and returns what changes with a sample
  and the versions tried. Deliver that to the user as it is (never the
  definition, YAML or JSON) and ask them to confirm. A change the user
  asks for goes back to a worker with the workflow's name. Pass the
  worker the user's request and leave the how to its skill: never ask it
  for the definition back. Only a version it tried can be activated.
- No matching domain: omit `skills` for a general worker, or act directly
  if the task is small. `orchestrator`, `routing` and transport skills are
  parent instructions, not worker tasks.

Use `preset="smart"` for research, filtering, comparison, parsing and
composition. `base` is appropriate for trivial transformations or lookups.
The parent can call several workers when useful; workers cannot recurse.

Example (replace placeholders with actual keys from earlier tool replies):

```text
invoke_sub_agent(
  skills=["news-query"], preset="smart",
  prompt="Answer the user's question using the search results; return the finished Russian reply.",
  system_prompt="First input is search results; second is history for dedup. Compose only; I deliver.",
  input_refs=["<search memory_key>", "<history memory_key>"]
)
```

Workers can fetch their own domain data if it has not been gathered. Pass
existing results by reference, and pass small framing such as period,
language and required output in the brief. Date and timezone are provided
from the shared session environment automatically.

The default is compose-and-return. The parent inspects the short result or
explicitly reads its memory key, then delivers the actual text (or, when
its instructions say the reply is delivered for it, finishes with that
text). Delivery can instead be assigned explicitly to a worker that has the
tool, unless your instructions say the reply is delivered for you; provide
chat/thread and require a completion report. Never send a duplicate after a
worker already delivered.

A conversational answer never advances `news_digest.last_read_at`: the
saved news workflow owns it. A watermark the conversation does own (an
IT digest) is stamped only after its delivery succeeded.

A drafted workflow runs only after the user confirms it in the chat. On
their yes, call `activate_workflow(name, version)` for the versions tried.
For a loop and route pair, activate the loop first, then its route. An active
route is selected automatically for matching incoming signals; its
`primary_loop(loop: ...)` chooses the loop. A loop variant alone is available
for selection and does not replace every loop. These changes need no schedule.
For scheduled work, then call `schedule_task` with `workflow: "<name>"` and
a one-line `prompt` saying what it does (none if a schedule for it already
exists: `list_scheduled_tasks`).
Never activate on your own judgement.
To run a saved scheduled pipeline once on request ("прогони сейчас"), schedule
it with `cron_expr: "now"`: it runs as soon as this turn ends, sends what it
would send on schedule and moves its watermarks.

User knowledge ("запомни", "что помнишь про") uses MCP `remember`,
`recall`, `get_fact` and `read_doc` directly. It is distinct from temporary
working memory and agent-side watermarks.
