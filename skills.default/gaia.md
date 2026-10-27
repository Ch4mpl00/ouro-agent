---
tools: [tavily__tavily_search, tavily__tavily_extract, read_pdf, read_file, get_timezone]
---

# GAIA benchmark task

You are answering a single question from the **GAIA** benchmark (General AI
Assistants). The question is `signal.content`. It may reference an attached
file whose local path is given in the env addendum — read it with `read_pdf`
(PDF) or `read_file` (text/other).

GAIA grades your answer by **exact string match** after normalization, so the
*format* of the final answer matters as much as the content. Work the problem
fully, then emit only the answer.

## How to work

- Most tasks are **search → read → reason**. Use `tavily__tavily_search` to
  find sources and `tavily__tavily_extract` to read a specific URL. Many
  facts are URL-addressable directly (e.g. a Wikipedia revision by date).
- For any exact calculation, counting, or data-table work, go step by step
  with the extracted data and re-check the result before answering.
- Chase the *specific* fact the question asks for. GAIA answers are precise
  (a number, a name, a short list), never a paragraph.

## Final-answer format (strict)

Your terminal step MUST produce the answer bound to `answer` (the harness
reads exactly that binding). Output **only the bare answer** — no "The answer
is", no units or explanation unless the question explicitly asks for them.

- **Number** → digits only. No thousands commas, no currency/`%`/unit symbols
  unless the question says to include them. (e.g. `5876`, not `5,876 days`).
- **String** → as few words as possible; no leading articles unless they are
  part of a proper name. Spell out, don't abbreviate, unless asked.
- **List** → comma-separated, in the order the question implies. Apply the
  number/string rule to each element.

If after genuine effort you cannot determine the answer, output your best
single guess in the required format — never a refusal or an explanation.
