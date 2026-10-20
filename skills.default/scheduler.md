---
routable: false
tools: *
---

# Scheduler signal handling

You are reacting to a `source=scheduler` signal in the primary agent loop.
The agent fires this when a task registered via `schedule_task` is due in
the configured timezone. A task naming a saved workflow runs that workflow
first; tasks without one come directly to you.

The signal `content` starts with a header block followed by the user's
own prompt body:

```
Scheduled task #<id> fired.
Cron: <expr>
Slot: <ISO>
Now: <ISO>
Previous fire: <ISO | "never (this is the first run)">
Recurring: yes | no (one-shot)

<the user's prompt verbatim from when they scheduled the task>
```

## Protocol

1. **Read the body** (everything after the blank line). It is the user's
   own words from when they set the reminder (e.g. *"напомни купить
   хлеб"*, *"check Monobank balance and message me if negative"*).

2. **Does the body map to a digest / delegated skill?** (per `routing.md`
   table — typical triggers: "IT-новости / tech-digest", "квитанции /
   nashdom-bill"). If yes, go
   to **§3 Delegated path**. Otherwise **§4 Inline path**.

3. **§3 Delegated path.** Fetch chat history if needed for digest dedup:

   ```text
   get_telegram_chat_history(chatId=<default chat id>, limit=30)
   ```

   Pass the result's `memory_key` to the domain worker:

   ```text
   invoke_sub_agent(
     skills=["tech-digest"], preset="smart",
     input_refs=["<history memory_key>"],
     system_prompt="Input is recent chat history for dedup. Return the finished Russian text; I handle delivery and bookkeeping.",
     prompt="<the scheduled task body>"
   )
   ```

   The worker gets date, timezone and news watermark from the session.
   It can fetch missing domain evidence itself. Use multiple workers when
   useful, passing result keys between them instead of copying payloads.
   Read `content`, or explicitly read its memory key if the result was
   large, then send the finished text with `send_telegram_message`.
   Only after a successful tech-digest delivery, update
   `tech_digest.last_read_at`. A topical query does not advance it.
   If delivery was explicitly assigned to a worker, inspect its completion
   report and do not deliver again.

4. **§4 Inline path — handle the task yourself.**
   - **Reminder / notification.** Send one short Russian Telegram
     message to the default chat. Don't prefix with `[reminder]` or
     similar noise — the message stands on its own.
   - **Action** ("check X / fetch Y / post Z"). Perform with whatever
     tools fit, then send a Telegram summary.

5. **One signal = one user-visible outcome.** Whether delegated or
   inline, end with exactly one outgoing Telegram message.

6. **Recurring tasks fire again automatically.** Don't try to
   re-schedule them. One-shot tasks auto-deactivate after this single
   fire — check the `Recurring: …` header.

For recurring deterministic work, delegate to `workflow-author` to draft
and try a saved workflow, then present its behavior and sample to the user.
Activate it and update the schedule only after their confirmation, following
`workflow-author`. Simple reminders can stay as scheduled prompts.

## Style

- Russian, terse, friendly. Same conventions as `telegram.md` (no
  tables, no Markdown, no t.me links).
- The user already knows what they asked you to remind them of — don't
  echo the cron expression or the scheduling metadata back.
