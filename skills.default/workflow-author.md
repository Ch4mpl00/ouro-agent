---
tools: []
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
2. Find what you will build on. `workflow_functions` lists every function
   a workflow can call; `workflow_functions(name)` shows one in full.
   `list_skills` shows the skills; read the ones that hold rules for this
   kind of task (categories, tone, prompts already tuned for it) and reuse
   them rather than inventing your own. This skill is already loaded, and
   skills that steer the agent itself (routing, transports) hold nothing a
   workflow runs: skip them.
3. Call every read function the workflow will call with `probe_function`,
   with the arguments the workflow will pass, and build each expression from
   the fields you saw in the reply. Do not skip this: `save_workflow`
   checks argument names, not the fields of replies, so a wrong field is
   just `null` at run time and the workflow quietly does the wrong thing on
   every run. The reply is the only truth about names, units and meanings:
   never fill them in from what you know of the service behind a function.
   A probe shows the shape of the data, not the data a run will see: never
   write the values it happened to return (texts, names, ids, amounts)
   into the definition; judge what matters by criteria (`llm_decide`) or
   by fields, never by the sample's words.
4. Design it with the principles below, write the definition and call
   `save_workflow(name, definition)`. It checks the definition against the
   language, the functions and their argument names. Fix every reported
   problem and save again until it is accepted.
5. Return to the parent, in Russian: what the workflow does step by step,
   which calls change something (the reply's `calls` with
   `effect: "write"`), what it does when there is nothing new, the schedule
   you propose, and the name and version. Do not activate and do not
   schedule: the user confirms first, and the parent does both after they
   agree.

To change a workflow: `read_workflow`, edit, `save_workflow` (a new
version), and the same confirmation. Schedules name the workflow, not a
version, so they run whatever version is active.

## Designing one

A workflow is a fixed pipeline; its model steps are the only places that
judge. Build the pipeline the task needs from these parts, no more:

- **Sources.** The read functions that hold the data, scoped to what is
  new since the previous run. Prefer a function's own filters (a time
  bound, a category, a limit) to fetching everything and filtering later.
  Read independent sources in one `fork`.
- **Window.** A run covers the time since the previous one: a lookback
  that matches the schedule, or a watermark. Avoid calendar-day arithmetic
  in jq: `env.now` is UTC, and the user's day is not.
- **State between runs.** The input carries two watermarks,
  `env.newsLastReadAt` and `env.techLastReadAt`, stamped with `set_memory`
  under `news_digest.last_read_at` / `tech_digest.last_read_at`; any other
  key `set_memory` writes cannot be read back by a workflow. For anything
  else, keep a journal stream named after the workflow: `journal_append`
  what you delivered, `journal_tail` it at the start, and let the model
  steps leave out what it already covers.
- **Selection.** Many items of which some matter → `llm_decide` with the
  criteria spelled out (what to take, what to drop). Items an expression
  can judge (a field, a threshold, a date) → jq, no model.
- **Computation.** Sums, counts, filters, sorting and grouping are jq
  (`[.rows[] | select(.value < 0) | .value] | add`), never a model step: a
  model adds cost and can miscount. Hand the model the computed numbers and
  let it only write the text.
- **Synthesis.** Text for a person → `llm_compose`, with a skill that holds
  this kind of text's rules when there is one. Pass the data in `input`,
  keep the prompt to what this run adds. `preset: smart` for what the user
  reads, `base` for mechanical reshaping.
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

## The language

A subset of the Open Workflow Specification 1.0, in YAML. Anything not
listed here is rejected (`listen`, `emit`, `run`, `schedule`, catalogs,
secrets, authentication).

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

And any of: `if: '${ … }'` (false → skipped, its input passes on),
`input: { from: '${ … }' }` (what the task sees), `output: { as: '${ … }' }`
(what it returns), `export: { as: '${ … }' }` (the new `$context`),
`timeout: { after: <duration> }`, `then: <flow>`, and on a `call`
`metadata: { delivery: at_most_once }`.

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

Data flow: each task's output is the next task's input. The workflow's
input is `{ env: { timezone, now, newsLastReadAt, techLastReadAt,
userEmail, telegramChatId }, signal: { source, content, id } }`, available
everywhere as `$workflow.input`. Keep a result for later tasks with
`export: { as: '${ $context + { name: . } }' }` and read `$context.name`.

Functions besides the ones `workflow_functions` lists from the tools:

- `llm_compose { skill?, prompt?, input?, preset? }` — one generation with
  that skill; `input` is an object shown to the model as tagged sections;
  `preset` is `base` (default), `smart` or `smartest`. Returns the reply,
  parsed when it is JSON.
- `llm_decide { items, prompt, threshold? }` — keeps the items of the array
  that the decision model judges relevant by `prompt`.
- `set_memory { key, value }` — stamps one of the two watermarks above.

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
        preset: smart
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
