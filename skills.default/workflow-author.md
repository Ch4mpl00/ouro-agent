---
tools: *
---

# Authoring saved workflows

A saved workflow is a recurring task written down once as a definition and
then run on a schedule without an agent deciding what to do next. Only its
explicit `llm_compose` / `llm_decide` steps call a model. Write one when the
user wants something done regularly ("присылай каждое утро …", "раз в
неделю проверяй …"), or when an existing scheduled task should stop being
re-planned on every fire.

## Procedure

1. Pin down the ask: what the user wants to receive or have done, from
   which data, how often, and what counts as new or relevant. Ask the
   parent back only when a choice would change what the user gets.
2. Learn what you will build on. `list_skills` and `read_skill` the skills
   of the domain: they hold the categories, tone and rules the user already
   gets elsewhere, and `planner` holds tuned prompts for the tasks it runs.
   Then call every read tool the workflow will call, with the arguments it
   will use, and build each expression from the fields you saw in the
   reply. Do not skip this: `save_workflow` checks argument names, not the
   fields of replies, so a wrong field is just `null` at run time and the
   workflow quietly does the wrong thing on every run. The reply is the only
   truth about names, units and meanings: never fill them in from what you
   know of the public API behind a tool.
3. Design it with the principles below, then write the definition and call
   `save_workflow(name, definition)`. It checks the definition against the
   specification, the functions this agent has and their argument names.
   Fix every reported problem and save again until it is accepted.
4. Return to the parent, in Russian: what the workflow does step by step,
   which calls change something (the reply's `calls` with
   `effect: "write"`: sending messages, writing memory), what it does when
   there is nothing new, the schedule you propose, and the name and
   version. Do not activate and do not schedule: the user confirms first,
   and the parent does both after they agree.

To change a workflow: `read_workflow`, edit, `save_workflow` (a new
version), and the same confirmation. Schedules name the workflow, not a
version, so they run whatever version is active.

## Designing one

A workflow is a fixed pipeline; its model steps are the only places that
judge. Build the pipeline the task needs from these parts, no more:

- **Sources.** The read tools that hold the data, scoped to what is new
  since the previous run. Prefer a tool's own filters (`sinceISO`,
  `source`, `channel`, `limit`) to fetching everything and filtering later.
  Read independent sources in one `fork`.
- **State between runs.** The input carries two watermarks,
  `env.newsLastReadAt` and `env.techLastReadAt`, stamped with `set_memory`
  under `news_digest.last_read_at` / `tech_digest.last_read_at`. Any other
  key `set_memory` writes cannot be read back by a workflow. For anything
  else, keep a journal stream named after the workflow: `journal_append`
  what you delivered, `journal_tail` it at the start, and let the model
  steps leave out what it already covers.
- **Window.** A run covers the time since the previous one. Use the tool's
  lookback that matches the schedule (`days: 1` for a daily run) or a
  watermark (`sinceISO`), not calendar-day arithmetic in jq: `env.now` is
  UTC, and the user's day is not.
- **Selection.** Many items of which some matter → `llm_decide` with the
  criteria spelled out (what to take, what to drop). Items an expression
  can judge (a field, a threshold, a date) → jq, no model.
- **Computation.** Sums, counts, sign filters, sorting and grouping are jq
  (`[.transactions[] | select(.amount < 0) | .amount] | add`), never a
  model step: a model adds cost and can miscount. Hand the model the
  computed numbers and let it only write the text.
- **Synthesis.** Text for a person → `llm_compose`, with the domain's skill
  when there is one, so the voice and categories are the ones the user
  knows. Pass the data in `input`, keep the prompt to what this run adds,
  and reuse a prompt the domain's skills or `planner` already tune instead
  of writing a new one. `preset: smart` for what the user reads, `base` for
  mechanical reshaping.
- **Delivery.** `send_telegram_message` to
  `$workflow.input.env.telegramChatId` unless the user named another chat;
  never write a chat id into the definition.
- **Bookkeeping last.** Stamp a watermark or append to a journal only after
  the delivery it stands for succeeded, so a failure cannot mark unsent
  data as handled.
- **Nothing new.** Decide it explicitly: skip the delivery with `if`, or
  send a short note. For a frequent schedule a note on every quiet run is
  noise; say which you chose.
- **Few model steps.** Every `llm_*` call costs time and money on every
  run. One `llm_decide` and one `llm_compose` cover most digests.
- **Schedule.** One task, all its times in one cron (`0 9,19 * * *`), on
  the user's clock. Name the workflow after what it does, lowercase with
  dashes.

## Runs and failures

Once a run starts, its definition and input are pinned. Restart reuses saved
step results; `wait` and retry delays release the worker and resume from
persisted deadlines. A run that fails before any write falls back to the
agent; one that fails after a write is only reported.

External calls default to `at_least_once`: a lost response can cause a
duplicate effect. For calls where a duplicate is unacceptable, set
`metadata: { delivery: at_most_once }` on the `call` task. An unknown outcome
then stops the run without retry; execution can be lost if the agent crashes
before dispatch. Put this metadata on external calls inside reusable
functions, not on their callers. `llm_compose` and `llm_decide` currently
support only the default policy. Do not wrap unknown outcomes in retries.

A `try` retry does not repeat a write that already succeeded: the retry
reuses its saved result and goes on, while a write that failed is sent
again. If the retried attempt would send different arguments (text a model
composed again inside the `try`), the run fails with a conflict and sends
nothing, so keep each `try` narrow, around the call it retries, with the
`llm_compose` before it. Reads and model steps run again on every attempt.

## The definition

Open Workflow Specification DSL 1.0, YAML. The supported subset:

- Tasks: `call`, `set`, `do`, `for`, `switch`, `fork`, `try`/`catch`
  (with `retry`), `raise`, `wait`. Each may have `if`, `input.from`,
  `output.as`, `export.as`, `timeout` and `then` (`continue`, `exit`,
  `end`, or a task name in the same list). A task whose `if` is false is
  skipped and passes its input on.
- `switch` is a list of `- name: { when: '${ … }', then: <flow> }`; the
  first case whose `when` holds decides, and a case without `when` is the
  default.
- Not supported: `listen`, `emit`, `run`, `schedule` (schedules are
  `schedule_task`), catalogs, secrets, authentication.
- `document: { dsl: '1.0.4', namespace: agent, name: <name>, version:
  '1.0.0' }`; `name` is lowercase with dashes and equals the saved name.

Expressions are jq inside `${ … }`. A value is either text or one whole
expression: `"Привет ${ .name }"` is an error, write
`'${ "Привет " + .name }'`. Quote values that start with `${` in YAML.

Data flow: each task's output is the next task's input (`.`). The
workflow's input is `{ env: { timezone, now, newsLastReadAt,
techLastReadAt, userEmail, telegramChatId }, signal: { source, content,
id } }`, available everywhere as `$workflow.input`. To keep a result for
later tasks, export it into the context:
`export: { as: '${ $context + { digest: . } }' }`, then read
`$context.digest`. A `fork` returns its branches' outputs as an array in
order.

Functions (`call`): every MCP tool you can see, with its arguments under
`with`, and:

- `llm_compose { skill?, prompt?, input?, preset? }` — one generation with
  that skill; `input` is an object shown to the model as tagged sections;
  `preset` is `base` (default), `smart` or `smartest`. Returns the reply,
  parsed when it is JSON.
- `llm_decide { items, prompt, threshold? }` — keeps the items of the array
  that the decision model judges relevant by `prompt`.
- `set_memory { key, value }` — stamps one of the two watermarks above.

## Syntax at a glance

The constructs above in one place: a fork of reads, a journal for what was
already sent, a skipped step. An illustration of the syntax, not a template
to copy: design each workflow from its own task.

```yaml
document: { dsl: '1.0.4', namespace: agent, name: rust-releases, version: '1.0.0' }
do:
  - gather:
      fork:
        branches:
          - found:
              call: search_news
              with: { query: 'Rust programming language release', k: 10 }
          - told:
              call: journal_tail
              with: { stream: rust-releases, n: 5 }
      output: { as: '${ { found: .[0].results, told: [.[1].entries[].body] } }' }
      export: { as: '${ . }' }
  - relevant:
      call: llm_decide
      with: { items: '${ .found }', prompt: 'A new Rust release or release candidate, not commentary about one.' }
  - write:
      if: '${ length > 0 }'
      call: llm_compose
      with:
        preset: smart
        prompt: 'Two lines per release, in Russian. Leave out what input.told already reported; nothing left → an empty reply.'
        input: { releases: '${ . }', told: '${ $context.told }' }
      export: { as: '${ $context + { text: . } }' }
  - send:
      if: '${ ($context.text // "") != "" }'
      call: send_telegram_message
      with: { chatId: '${ $workflow.input.env.telegramChatId }', text: '${ $context.text }' }
  - remember:
      if: '${ ($context.text // "") != "" }'
      call: journal_append
      with: { stream: rust-releases, body: '${ $context.text }' }
```
