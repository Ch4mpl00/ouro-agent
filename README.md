<p align="center">
  <img src="https://github.com/user-attachments/assets/e9695e32-8317-44c5-9f2a-b8f72da6ed94" width="140" alt="Ouro helper logo" />
</p>

<h1 align="center">Ouro helper</h1>

> A personal agent that reads your mail, chats with you in Telegram, watches
> your news and channels, answers other agents, and remembers. Separate Rust
> processes that talk to each other only over the
> [Model Context Protocol](https://modelcontextprotocol.io). Its agent loop
> is itself a durable workflow.

<p align="center">
  <a href="docs/architecture.md"><img src="https://img.shields.io/badge/status-work%20in%20progress-orange" alt="Status" /></a>
  <a href="docs/architecture.md"><img src="https://img.shields.io/badge/Rust-2024-b7410e" alt="Rust" /></a>
  <a href="https://modelcontextprotocol.io"><img src="https://img.shields.io/badge/protocol-MCP-blueviolet" alt="MCP" /></a>
  <a href="https://open-workflow-specification.org/"><img src="https://img.shields.io/badge/workflows-Open%20Workflow%20Spec-2e7d32" alt="Open Workflow Specification" /></a>
  <a href="docs/architecture.md#memory"><img src="https://img.shields.io/badge/memory-pgvector-336791" alt="pgvector" /></a>
</p>

> 🚧 **Work in progress.** This is a personal project under active
> development, and its interfaces and internals change often.

---

## Meet Ouro

Ouro is a personal agent that lives on a small server and works for one
person. You talk to it in Telegram the way you would talk to an assistant.
It also keeps an eye on things for you without being asked: bills arriving
by mail, news worth knowing, tasks you scheduled for later.

It is not a chatbot bolted onto a few APIs. Ouro decides how to handle each
request itself. It can delegate to sub-agents, write and run small
programs, remember what matters, and over time turn what it has learned
into new skills and automations of its own.

## What it does

| | |
|---|---|
| 💬 **Talks to you in Telegram** | Answers questions and carries out requests, using the conversation's history as context. |
| 📰 **Reads the news for you** | Collects Hacker News, Habr and the Telegram channels you follow, and sends digests that skip what you have already seen. |
| 🧾 **Handles your mail** | Notices utility bills in Gmail, reads the PDF and tells you what to pay. |
| 🧠 **Remembers** | Keeps projects, notes and facts in a memory it shares with your other AI tools, and finds them by meaning, not just by keywords. |
| ⏰ **Keeps time** | Reminders, recurring checks and scheduled reports, set up in plain words. |
| 🤝 **Works with other agents** | Other assistants, such as Claude Code, can message Ouro and get answers back. |

## It grows with you

Ouro's behaviour is written in plain-text **skills**: how to talk in a chat,
what a good digest looks like, when to hand work to a helper. Ouro can read
and rewrite its own skills. Ask it to change how it does something, and it
updates the skill instead of forgetting by the next conversation.

Routine work becomes **workflows**. When a task repeats, Ouro drafts a
workflow for it, tries it out safely first, shows you what it would do, and
then runs it on schedule. A workflow doesn't need a model to decide every
step, so it is cheaper and more predictable.

Every night Ouro looks back over its recent work and decides what to
improve in its own skills.

## The idea behind it

> **Give the agent as much freedom and generality as possible.**

The system around Ouro informs it and never acts on its behalf. Behaviour
lives in skills Ouro can change, not in code it can't. One general
mechanism, such as memory, skills, workflows or helpers, is always preferred
to a special case. The goal is an assistant that becomes more capable as it
learns, not one that is limited to the cases someone thought of in advance.

**The unit of work is an agent loop.** A skill supplies the instructions
and tool scope; the loop runs them with a transcript, working memory and
execution state. Delegating to a specialist starts another loop with its
own skills and context. The same mechanism serves the primary agent and
its workers.

## The agent loop is a workflow

Ouro has its own Rust interpreter for the
[Open Workflow Specification](https://open-workflow-specification.org/).
It validates definitions against the official JSON Schema and executes
the subset Ouro uses: branching, loops, reusable functions, fork/join,
retries, timeouts and event waits, with jq expressions for data flow.
The interpreter lives in [its own crate](crates/owf), independent of the
agent and MCP.

The same engine runs scheduled automations **and the agent itself**:

- [`agent-loop.yaml`](crates/agent/workflows/agent-loop.yaml) defines the
  cycle: ask the model → review calls → run tools or child agents → fold
  results into context → continue or answer.
- [`signal.yaml`](crates/agent/workflows/signal.yaml) defines how an event
  becomes work: gather context, choose a saved workflow or the primary
  loop, and request a recovery report if it fails.
- [`guard.yaml`](crates/agent/workflows/guard.yaml) coordinates call review
  and confirmation waits. Rust verifies the evidence that authorizes each
  call.

Ouro can propose and activate new versions of `agent-loop` and `signal`
with your approval, changing its own orchestration without a Rust rebuild.
Existing runs keep their pinned definitions. Three consecutive faults in
an override restore the built-in version for new runs.

Skills describe how to do the work; workflows describe how execution
proceeds. Both are readable definitions the agent can work with. A task
can stay an open-ended model/tool loop, become a fixed pipeline with model
steps only where judgement is needed, or delegate to another loop.

## Durable by design

The primary loop, its workers and saved workflows persist their progress
in SQLite. Restarting the process resumes admitted runs from checkpoints:
completed steps restore their results, and agent loops recover their
transcript and working memory.

- **Waiting survives restarts.** Timers, retry deadlines and event waits
  are persisted. A suspended run releases its worker, so waiting for your
  confirmation does not block the next conversation.
- **Children survive with their parent.** A resumed parent rejoins the
  same durable child instead of starting a replacement. Ending the parent
  cancels unfinished children.
- **Recovery has ownership.** Fenced leases prevent an expired worker
  from committing checkpoints after another worker takes over.
- **Side effects have explicit delivery rules.** Saved workflows default
  to at-least-once calls and can opt into at-most-once dispatch. The agent
  loop protects tool writes with at-most-once dispatch; if a crash leaves
  the outcome unknown, it tells the model rather than resending the call.

At-most-once dispatch can lose execution if the process dies before the
call goes out; at-least-once can repeat a call whose response was lost.
The [durability contract](crates/owf/README.md) spells out these boundaries.
Recovery is tested with injected checkpoint failures, property tests and
real SIGKILLs across dispatch and commit boundaries.

## How it fits together

```mermaid
flowchart LR
    E[Mail · Telegram · other agents · schedule] --> O((Ouro))
    O --> S[Skills]
    O --> W[Workflows]
    O --> M[Memory]
    O --> T[Tools: Telegram, mail, news, files, sandbox]
```

Events arrive as signals. Ouro picks them up one at a time, decides what to
do, and acts only through tools. Everything it does is traced, so any
decision can be looked at afterwards.

## Learn more

- [Architecture](docs/architecture.md) explains how the system is built.
- [Operations](docs/operations.md) covers running it, tests and deployment.

<sub>A personal project, built in Rust with a lot of help from AI coding agents.</sub>
