# Optimizer workbench

`optimize` proposes a smaller task-specific OWF workflow from full Langfuse
traces, then uses the existing guarded agent loop to test and refine it with
local tools. Behavior lives in `skills.default/workflow-optimizer.md`. The CLI
never connects to a live MCP transport, opens production agent stores, saves or
activates a workflow, schedules work, or deploys anything.

**Current execution scope: standalone pipelines.** Existing `llm_compose` and
other standalone function contracts are reused. JQ, control flow, reusable
functions and bounded concurrent `for`/`fork` run in the OWF interpreter.
Signal/agent/guard/child/event host functions need admitted runtime context and
are rejected before execution, including when a fixture could answer them.
The existing signal sandbox remains separate and retains its original behavior.
A proposal containing an agent loop can still be produced with `--one-shot`;
the workbench cannot claim to have tested it.

## Invocation

Run the existing binary (or use `cargo run -p evals --bin optimize -- ...` where
compilation is allowed):

```sh
target/debug/optimize TRACE_ID --model openrouter/deepseek/deepseek-v4.1-flash \
  --out /tmp/optimization.md --artifact /tmp/optimization.json

target/debug/optimize --trace-file /tmp/full-trace.json --mock-only \
  --max-iterations 30

target/debug/optimize --scenario /tmp/scenario.json --mock-only \
  --trace-file /tmp/full-trace.json --catalog /tmp/catalog.json \
  --artifact /tmp/scenario-report.json

target/debug/optimize TRACE_ID --one-shot --out /tmp/proposal.md
target/debug/optimize TRACE_ID --dump --out /tmp/prompt.md
```

`--definition FILE` changes the reference definition (default: agent-loop).
Multiple trace ids and `--trace-file` flags are supported. Trace files contain
the full Langfuse trace API object with its complete `observations` array;
shortened inspector output is not a fixture source. Trace ids use Langfuse
credentials; local files and mock-only scenarios require no model/network keys.
`--dump` performs no model calls. `--one-shot` keeps the original one-generation,
no-tools behavior, with YAML syntax/schema verification only.

The optimizer model defaults to `AGENT_SMARTEST_MODEL`, otherwise the default
smartest preset, with maximum reasoning. It must support tool calls in iterative
mode. `--model` changes the outer optimizer only. Candidate `llm_compose` uses
its own selectable `base`, `smart`, `smartest` or `compose` preset from the normal
defaults and `AGENT_*_MODEL` overrides. `--compose-model` overrides only the
candidate's `compose` model; a candidate explicitly selecting `preset: base`
still uses base. Capabilities record the model and reasoning effort of each
candidate preset. Providers are configured independently by family; unavailable
credentials fail only an actual generation using that family. Mock-only mode
removes candidate real permissions and keeps the outer optimizer available.

The optimizer has at most **30 turns**, including its final response.
`--max-iterations` can lower that bound. There is no aggregate token,
generation-count or overall wall-time ceiling by default. `--timeout-seconds`
optionally sets an overall timeout when explicitly requested; the model receives
the configured turn/time limits in its execution context. Each generation still
uses the runtime's existing timeout. A scenario has at most 10,000 tasks,
10 retry attempts, no actual waits/retry sleeps, and a wall-time limit of
120 seconds (or less when the explicit CLI timeout is shorter). Ctrl-C cancels
scoped work. Token and cached-token usage are provider-reported statistics,
not execution limits; unknown usage is identified by calls_with_usage.
No prices or monetary cost ceiling are inferred.

## Host policy and exact fixtures

For every evaluated call:

1. A matching explicit fixture wins.
2. Otherwise an explicitly permitted read-only host adapter may execute.
3. Otherwise the workbench stops with `needs_mock`, including the function,
   evaluated JSON arguments and task path. No live fallback exists.

The CLI's real allow-list contains only `llm_compose`; `--mock-only` empties it.
The optimizer can inspect it but cannot change it. Library hosts can inject
other safe adapters and allow-list them; writes cannot be permitted real,
including adapters whose per-call effect becomes a write. Catalog membership
alone never grants real permission.

```json
{
  "yaml": "document: {dsl: '1.0.4', namespace: demo, name: task, version: '1.0.0'}\ndo:\n  - read: {call: fetch_url, with: {url: 'https://example.test/feed'}}\n",
  "input": {},
  "label": "recorded read",
  "mocks": [{
    "function": "fetch_url",
    "arguments": {"url": "https://example.test/feed"},
    "response": {"kind": "recorded", "reference": "trace:TRACE/observation:OBS/output", "wire": true},
    "provenance": "recorded",
    "note": "Full response from the reference trace"
  }]
}
```

Other responses are `{"kind":"result","value":...}` (the exact
workflow-visible JSON value) or `{"kind":"error","error":{"type":
"https://open-workflow-specification.org/spec/1.0.0/errors/communication",
"status":403,"detail":"source blocked"}}`. Provenance is `recorded`,
`adapted` or `synthetic`; use adapted/synthetic when changing an observed
response or constructing a hypothetical scenario.

Matching ignores object-key order recursively, while preserving JSON values,
types and array order. It never depends on arrival order in parallel work.
Duplicate exact fixtures are rejected. Different arguments legitimately need a
new fixture. Missing fixtures are host-level interruptions outside YAML
`try/catch`; an injected error response is an ordinary catchable workflow error.
If any branch encounters a missing fixture, the report is `needs_mock` even if
another branch wins a competing fork.

Recorded fixtures resolve complete original observations, never compact heads.
With `wire: true`, raw MCP strings use the existing JSON/string conversion;
`[tool error]` replies become communication errors. Older traces containing
already decoded objects remain objects. With `wire: false`, the recorded value
is returned without wire decoding (a JSON-encoded string remains a string),
while recorded ERROR observations remain failures. Explicit OWF Problem Details
retain their type/status; recorded transport-error objects use the existing communication/503 conversion and retain their text.
This bench does not simulate live delivery uncertainty.

Tools observed in traces form a conservative catalog: names are known, schemas
and effects may be unknown, so they default to would-be writes. Such schemas
are listed in each report and cannot establish production contract compatibility.
Synthetic agent tools are excluded from this inferred external catalog. A host
can supply a catalog JSON array with entries like:

```json
[{"name":"fetch_url","effect":"Read","parameters":{"type":"object","properties":{"url":{"type":"string"}},"required":["url"],"additionalProperties":false},"provenance":"host-supplied production schema"}]
```

The normal agent workflow checker validates syntax/schema, JQ, known names and
argument names/requirements; it does not claim complete runtime type validation
for every external tool. A fixture cannot create an unknown function. Effects
are audit classifications here, not permission grants.

## Reruns, tools and artifacts

`optimizer_capabilities` exposes policy, catalog, settings and limits.
`optimizer_evidence` reads full observations or a stored trial.
`optimizer_try` accepts complete YAML/input/mocks and returns an execution
report. The optimizer adds fixtures or changes the candidate and tries again.
All fixtures must be supplied on each try.

Each try starts with fresh workflow state. Successful allowed-real results are
cached in memory by candidate YAML, input, model settings, task reference,
function and canonical arguments. Adding/changing fixtures alone preserves
successful model responses; explicit mocks still override them. YAML/input
changes reset the cache, including comment-only YAML changes. Distinct task
paths remain distinct model invocations. Errors are not cached. The cache is
neither a durable resume nor shared across CLI processes. Use explicit model
fixtures to explore alternate branches reproducibly.

The model receives compact results with `trial:N` and
`trial:N/call:INDEX/output` refs (call indices start at zero). It can fetch full
reports, individual call arguments/results/errors and original observations
through `optimizer_evidence`. Engine replies over 8 KB are saved in session
working memory; `working_memory_get` is admitted so the optimizer can load the
complete reply rather than reason from a 512-byte preview.

The JSON artifact contains version, mode, proposal/error, host capabilities,
every complete scenario and report, aggregate usage and unknown monetary cost.
Each report records output, faults, full task path/events, would-be writes,
unknown schemas, and real/mocked/cached/missing calls with exact evaluated
arguments and provenance. Candidate usage is reported when a meter is attached;
null means unavailable. Interrupted in-flight real calls remain in the report.
`final_candidate_completed` identifies whether the exact final YAML completed
in a stored scenario; this is fixture evidence, not approval or equivalence.

`--artifact` picks the file. With `--out REPORT.md`, the default artifact is
`REPORT.artifact.json`; otherwise it uses a unique file in the system temp directory.
Execution failures retain trials in the artifact. Invalid YAML/function contracts
become invalid trial reports so the optimizer can correct them. The CLI preserves
a final invalid proposal before reporting validation failure. Artifacts may
contain private trace content; keep them local and review before sharing.
