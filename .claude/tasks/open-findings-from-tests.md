# Open findings from the test-coverage loop

**Status:** open. These are bugs and contract questions the TypeScript
test-coverage loop found (2026-10-12/13) and deliberately did not fix. The
TypeScript code was ported one to one to Rust, so most of them should still be
there; the first five were confirmed against the Rust code on 2026-10-20, the
rest need a re-check first (a finding that no longer reproduces is simply
dropped). The loop's fixes all landed and its log is in git history (this file
was `test-coverage-loop.md`).
**Priority:** P2
**Area:** mcp / agent (tests found them)
**Created:** 2026-10-13 (rewritten 2026-10-20)

## Context

The loop wrote integration and property tests through the real boundaries and
fixed every unambiguous bug in a separate commit. What follows needed a
decision (a contract change, or the owner's call), so the failing check was
left out or skipped with a pointer to the finding. Where a Rust test already
pins one, say so when you pick it up.

## Confirmed in the Rust code (2026-10-20)

- **`list_signals(since)` and the second-precision wire format**
  (`crates/mcp/src/signals.rs`). `created_at` goes out truncated to seconds, but
  `since` compares at microsecond precision. Copying a listed signal's
  `created_at` into `since` (as the tool's own text advertises) returns that
  signal again plus its same-second siblings. Truncating the comparison would
  lose later same-second signals; the real fix is a contract change: an id
  cursor (`after_id`) or sub-second timestamps.
- **`append_doc` can answer "raced" under contention** (`memory.rs`,
  `APPEND_CAS_ATTEMPTS = 3`). With 4 or more concurrent appends to one document
  (one model turn issuing parallel `append_doc` calls is enough) some fail,
  though the comment says append "absorbs the race". The reference model allows
  a raced write, so decide: retry until it lands (each lost round means another
  append won), or keep the bound and say so in the tool description. The model
  (`crates/mcp/tests/model`) is the user's: a rule change starts there.
- **`fetch_article` is reachable from the ChatGPT tunnel and persists what it
  fetches.** See `mcp-auth-and-tool-scoping.md` (Open); decide there.
- **Two stores for the user's knowledge.** The `knowledge` toolset (`add_note` /
  `find_notes`, `knowledge_base_notes`) is still registered while skills send
  "remember" / "what do you remember" to unified memory (`remember` /
  `recall`), and `memory-import-notes` already copied the notes into facts. A
  model that picks `add_note` writes where `recall` never looks. Drop the
  toolset from the default surface (or the module).
- **One article, two rows.** `fetch_article` stores a URL under `external`
  unless the host ends with `habr.com` / `ycombinator.com`
  (`source_for_url` in `news.rs`), but the HN poller stores the same article URL
  under `hackernews`: fetch it by hand first and the poller adds a second row
  (search shows it twice). Key articles by URL across sources (or look up any
  source before inserting). Also `ends_with("habr.com")` matches `evilhabr.com`.

## To re-check first

- **The Telegram poller is at-least-once across a failed write, and a crash
  between claiming a scheduler slot and recording its signal loses the slot.**
  The chat-log row, the signal and the cursor are three statements; a failure
  after the signal and before the cursor makes the next round redo the update
  (the agent gets the message twice). (The scheduler now lives in the agent
  (`schedule.rs`), which claims a slot before the fire runs, at most once; the
  Telegram half is still MCP's.) Proposal: a transaction seam in `Db` so
  signals, the Telegram log and the cursor write atomically. Changes `Db`:
  needs a decision.
- **An edited Telegram message is a new message.** The poller logs an
  `edited_message` as a new row and queues a new signal, so the agent answers
  the edit as a fresh request and history shows both versions. Possibly
  intended (re-ask by editing); decide.
- **The Gmail watermark is a timestamp.** A mail indexed by Gmail only after a
  newer one was already signalled, or a second mail with the same
  `internalDate` arriving a tick later, is behind the watermark and skipped for
  good; a crash between the signal and the watermark write signals a mail
  twice. Tracking seen message ids (bounded) next to the watermark closes all
  three.
- **`list_news` without `sinceISO` lists undated items first.** Descending
  `posted_at` puts NULLs first in Postgres; articles stored without a date
  (`fetch_article` on a page with none) come before every dated item and can
  push real news past the limit. `NULLS LAST` is probably what "newest first"
  means.
- **A channel can stall on media posts.** The harvest watermark is the highest
  stored message id and text-less posts aren't stored: 200 media-only posts in a
  row after the watermark come back every tick, store nothing, and the channel
  never moves past them. Keep a per-channel fetched-up-to cursor (or store media
  posts flagged).
- **Group messages already in the news store.** Until a fix, the channel
  harvester stored every supergroup the userbot is in as a "channel": those
  messages sit in `news_items` (source `channel`), embedded and searchable, also
  from the tunnel. Cleaning up needs the userbot's dialog list (which chat ids
  are supergroups) and a delete of their rows.
- **`fetch_url` treats every `application/x-*` as binary**, so `x-ndjson`,
  `x-yaml`, `x-javascript`, `x-www-form-urlencoded` come back as "binary content
  not returned". Which x- types are text is a judgement call (or sniff bytes).
- **An upstream down at MCP boot can keep the agent from starting.** The
  gateway lists upstream tools once at boot; an upstream unreachable then is
  skipped until MCP restarts. If a shipped skill declares its tools (the `gaia`
  skill names `tavily__*`; see the next item) the agent's boot validation fails
  and the supervisor crashes on every restart. Options: retry a failed upstream
  in the background, or treat missing gateway (`prefix__`) tools as a warning.
  A connect that loses the timeout race but later succeeds must not leave its
  client open.
- **Stale `gaia` skill.** `skills.default/gaia.md` serves the dropped GAIA
  benchmark effort; remove it (and re-check the previous item afterwards).
- **The dreaming flow reads untrusted text and can rewrite skills.** Confined to
  the dreaming signal, `write_skill` sits next to `list_signals`, whose contents
  are mail bodies, channel posts and Telegram messages: an instruction planted
  there can reach a skill at the next nightly run. A human review step (dreaming
  proposes, the user approves) or writing only an append-only overlay would
  close it.
- **`list_monobank_transactions` can't reach past 500.** Monobank returns at
  most 500 transactions newest first; the tool reports `complete: false` and
  `oldestReturned` but can't ask for the older part (the window always ends
  now; a second request costs the 60 s rate limit). Options: an optional
  `before` so the agent can page across calls a minute apart, or paging inside
  the tool with a wait.
- **A call whose MCP dies under it hangs for 60 s.** The server keeps no event
  store, so a POST's SSE stream has no resume point; when the process dies
  mid-call the client may only report a disconnect and leave the request pending
  until its timeout, blocking the supervisor. Check the Rust client
  (`crates/agent/src/mcp.rs`); a closed client should fail pending requests at
  once and reconnect.
- **Cron edge cases** (from the TS cron library; the Rust code uses `croner`, so
  re-test): a day-of-month that doesn't exist OR a weekday (`0 9 31 11 6`);
  30-minute DST shifts (Australia/Lord_Howe); and an evaluation error must not
  mean "invalid cron, skip the task forever".

## Acceptance

Each finding is either fixed with a test that fails without the fix, or
dropped here with the reason (does not reproduce / accepted as intended).
Specs (`*.feature`) and the memory reference model are the user's: a contract
change starts there or with the user.

## Notes

The testing principles the loop worked by, kept because they still apply:
integration first through real boundaries (fakes only at the edge: Telegram,
Gmail, Monobank, model providers, Langfuse, the clock); property tests judge
observed before/after states of the real system, not a second implementation;
rules per operation are outcome requirements, effect properties and frame
conditions (what must not change, also on refusal); observe persisted facts, do
not assume them; fail loudly on the unknown; one controlled clock tested at,
before and after every deadline; idempotency and concurrency (a concurrent
result must admit a legal sequential history) are behaviours of their own;
prove the checks bite with a planted bug; keep witnesses that the interesting
cases occurred; seeds replayable and runtime bounded (`PROPERTY_RUNS`,
`MODEL_CASES`).
