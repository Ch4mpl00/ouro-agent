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

1. Know the steps before writing them. If unsure which tools and arguments
   work, do the task once with the tools and look at their replies: a
   workflow refers to fields of those replies (`.items`, `.results`).
2. Write the definition and call `save_workflow(name, definition)`. It
   checks the definition against the specification, the functions this
   agent has and their argument names. Fix every reported problem and save
   again until it is accepted.
3. Return to the parent, in Russian: what the workflow does step by step,
   which calls change something (the reply's `calls` with
   `effect: "write"`: sending messages, writing memory), the schedule you
   propose, and the name and version. Do not activate and do not schedule:
   the user confirms first, and the parent does both after they agree.

To change a workflow: `read_workflow`, edit, `save_workflow` (a new
version), and the same confirmation. Schedules name the workflow, not a
version, so they run whatever version is active.

Once a run starts, its definition and input are pinned. Restart reuses saved
step results; `wait` and retry delays release the worker and resume from
persisted deadlines.

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
  `end`, or a task name in the same list).
- Not supported: `listen`, `emit`, `run`, `schedule` (schedules are
  `schedule_task`), catalogs, secrets, authentication.
- `document: { dsl: '1.0.4', namespace: agent, name: <name>, version:
  '1.0.0' }`; `name` is lowercase with dashes and equals the saved name.

Expressions are jq inside `${ … }`. A value is either text or one whole
expression: `"Привет ${ .name }"` is an error, write
`'${ "Привет " + .name }'`. Quote values that start with `${` in YAML.

Data flow: each task's output is the next task's input (`.`). The
workflow's input is `{ env: { timezone, now, newsLastReadAt,
techLastReadAt, userEmail }, signal: { source, content, id } }`, available
everywhere as `$workflow.input`. To keep a result for later tasks, export
it into the context: `export: { as: '${ $context + { digest: . } }' }`, then
read `$context.digest`. A `fork` returns its branches' outputs as an array
in order.

Functions (`call`): every MCP tool you can see, with its arguments under
`with`, and:

- `llm_compose { skill?, prompt?, input?, preset? }` — one generation with
  that skill; `input` is an object shown to the model as tagged sections;
  `preset` is `base` (default), `smart` or `smartest`. Returns the reply,
  parsed when it is JSON.
- `llm_decide { items, prompt, threshold? }` — keeps the items of the array
  that the decision model judges relevant by `prompt`.
- `set_memory { key, value }` — stores an agent watermark, e.g.
  `news_digest.last_read_at`.

Order writes so that a failure cannot lie: stamp a watermark only after
the delivery it stands for succeeded. A run that fails before any write
falls back to the agent; one that fails after a write is only reported.

## Example: the daily news digest

Copy the filter and compose prompts verbatim from the `planner` skill's
news-digest anchor (`read_skill planner`), and the chat id from the
environment.

```yaml
document: { dsl: '1.0.4', namespace: agent, name: news-digest, version: '1.0.0' }
do:
  - gather:
      fork:
        branches:
          - posts:
              call: list_news
              with: { source: channel, sinceISO: '${ $workflow.input.env.newsLastReadAt }' }
          - sent:
              call: journal_tail
              with: { stream: news-digest, n: 2 }
      output: { as: '${ { posts: .[0].items, sent: .[1].entries } }' }
      export: { as: '${ . }' }
  - filter:
      call: llm_decide
      with: { items: '${ .posts }', prompt: <the filter prompt of the planner's news-digest anchor> }
  - compose:
      call: llm_compose
      with:
        skill: news-digest
        preset: smart
        prompt: <the compose prompt of the planner's news-digest anchor>
        input: { posts: '${ . }', history: '${ $context.sent }', now: '${ $workflow.input.env.now }' }
      export: { as: '${ $context + { digest: . } }' }
  - send:
      call: send_telegram_message
      with: { chatId: 123456789, text: '${ $context.digest }' }
  - stamp:
      fork:
        branches:
          - watermark:
              call: set_memory
              with: { key: news_digest.last_read_at, value: '${ $workflow.input.env.now }' }
          - journal:
              call: journal_append
              with: { stream: news-digest, body: '${ $context.digest }' }
```
