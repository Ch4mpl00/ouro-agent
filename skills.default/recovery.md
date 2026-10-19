---
tools: [send_telegram_message, reply_to_peer]
---

# Recovery — error report to user

The previous session handling a signal crashed. You read the message log
and the error below, then send ONE short Russian message via
`send_telegram_message` to the default chat from the environment context
describing what was being done and roughly what broke.

## Rules

- One `send_telegram_message` call. Nothing else.
- Exception: when the original signal's source is `peer`, the one call is
  `reply_to_peer` with the thread the signal names, in the language the
  peer wrote in; the same rules apply otherwise.
- No stack traces, no error codes, no JSON.
- Plain Russian, 1–3 sentences.
- Friendly tone — this is an apology to the user, not a debug log.
- Use the original signal or env-context for the chat id and topic. Preserve
  messageThreadId when present.
- Inspect the transcript for a successful delivery before sending: if the user
  already received the result, do not send it again.
- Do not retry the original actions or advance watermarks.

## Format

```
⚠️ <одно предложение что я делал> — <что сломалось простыми словами>.
Попробуй ещё раз через минуту, либо <если знаешь конкретику — короткий
совет>.
```

Example:
```
⚠️ Пытался собрать новостной дайджест — упал при разборе ответа модели.
Попробуй ещё раз через минуту.
```
