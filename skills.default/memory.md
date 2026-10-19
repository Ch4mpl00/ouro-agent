---
tools: [remember, recall, get_fact, read_doc, update_fact, list_memory, create_project, write_doc, append_doc, patch_doc, doc_history, revert_patch]
---

# Personal knowledge: remember and recall

Save and retrieve personal facts the user asks you to remember through
MCP `remember` and `recall`. Handle these requests directly: they normally
need one tool call rather than a separate worker.

## Remember

Triggers include "remember that", "write this down", "make a note",
"do not forget", and "for future reference", in the user's language.

1. Make `body` self-contained: resolve pronouns and name the subject.
   For example, "Alex pays for internet on the first of each month",
   rather than "pays on the first". Resolve "remember this" against the
   supplied conversation history.
2. Choose 3–6 short lowercase retrieval tags covering people, topics,
   and objects, such as `["router", "password", "wifi"]`.
3. Call `remember(body=..., tags=[...], source="telegram")`.
4. Briefly confirm in the user's language; optionally restate the saved fact.

## Recall

Triggers include "what do you remember about", "what did I save",
"remind me", "what is my", and "when does", when asking about a saved
personal fact rather than news.

1. Call `recall(query=<the meaning of the question>)`. Add `tags` when
   the user specifies a clear category.
2. Follow relevant references with `get_fact` or `read_doc` as needed.
   Answer from saved evidence. If nothing relevant was saved, say so;
   do not invent an answer.

## Personal notes versus news

"What is new about OpenAI?" requests news. "What did I save about my...",
passwords, payment arrangements, addresses, and personal preferences request
personal knowledge. If a personal subject could mean either, try `recall`
first and search news only when no relevant personal fact exists.
