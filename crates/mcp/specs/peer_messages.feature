Feature: Talking to ouro from another agent
  Another agent (Claude Code, ChatGPT, …) talks to ouro through a peer
  endpoint: an instance serving the `peer` toolset. A message opens or
  continues a thread and becomes a `peer` signal; ouro answers in the thread
  with reply_to_peer, and the peer reads the answer with read_replies. The
  peer endpoint and the supervisor's server are separate instances sharing
  one database, so neither hears the other's in-process notifications.

  Background:
    Given a peer endpoint acting as "chatgpt"

  Scenario: A peer's message becomes a signal for ouro
    When the peer calls "ask_ouro" with:
      """
      { "text": "Что у меня в roadmap?", "from": "claude-code" }
      """
    And I remember "threadId" as "thread"
    And I call "get_next_signal"
    Then the result includes:
      """
      { "signal": { "source": "peer", "content": "Message from peer agent \"claude-code\" (via chatgpt), thread {{thread}}:\n\nЧто у меня в roadmap?" } }
      """

  Scenario: A peer that gives no name is still told apart by its endpoint
    When the peer calls "ask_ouro" with:
      """
      { "text": "ping" }
      """
    And I remember "threadId" as "thread"
    And I call "get_next_signal"
    Then the result includes:
      """
      { "signal": { "content": "Message from a peer agent (via chatgpt), thread {{thread}}:\n\nping" } }
      """

  Scenario: Ouro's reply reaches the peer
    Given the peer called "ask_ouro" with:
      """
      { "text": "Как дела?" }
      """
    And I remember "threadId" as "thread"
    When I call "reply_to_peer" with:
      """
      { "threadId": {{thread}}, "text": "Всё хорошо" }
      """
    And the peer calls "read_replies" with:
      """
      { "threadId": {{thread}} }
      """
    Then the result includes:
      """
      { "replies": [{ "text": "Всё хорошо" }] }
      """

  Scenario: Reading after a reply returns only newer ones
    Given the peer called "ask_ouro" with:
      """
      { "text": "Два вопроса" }
      """
    And I remember "threadId" as "thread"
    And I call "reply_to_peer" with:
      """
      { "threadId": {{thread}}, "text": "первый" }
      """
    And I remember "messageId" as "first"
    And I call "reply_to_peer" with:
      """
      { "threadId": {{thread}}, "text": "второй" }
      """
    When the peer calls "read_replies" with:
      """
      { "threadId": {{thread}}, "afterId": {{first}} }
      """
    Then the result includes:
      """
      { "replies": [{ "text": "второй" }] }
      """

  Scenario: A wait with no reply ends empty
    Given the peer called "ask_ouro" with:
      """
      { "text": "Ответишь?" }
      """
    And I remember "threadId" as "thread"
    When the peer calls "read_replies" with:
      """
      { "threadId": {{thread}}, "timeoutMs": 100 }
      """
    Then the result includes:
      """
      { "replies": [] }
      """

  Scenario: A waiting peer hears the reply at once
    Given the peer called "ask_ouro" with:
      """
      { "text": "Жду" }
      """
    And I remember "threadId" as "thread"
    When the peer waits up to 20 seconds for a reply in thread {{thread}} while I reply "Готово"
    Then the peer got "Готово" within 2 seconds

  Scenario: The supervisor's wait hears a peer at once
    When the supervisor waits up to 20 seconds for a signal while the peer asks "Ты тут?"
    Then the wait saw 1 pending signal within 2 seconds

  Scenario: Continuing a thread keeps the whole conversation
    Given the peer called "ask_ouro" with:
      """
      { "text": "Сколько будет 2+2?" }
      """
    And I remember "threadId" as "thread"
    And I call "reply_to_peer" with:
      """
      { "threadId": {{thread}}, "text": "4" }
      """
    And the peer called "ask_ouro" with:
      """
      { "threadId": {{thread}}, "text": "А умножить на 3?" }
      """
    When I call "get_peer_thread" with:
      """
      { "threadId": {{thread}} }
      """
    Then the result includes:
      """
      { "messages": [
          { "direction": "in",  "text": "Сколько будет 2+2?" },
          { "direction": "out", "text": "4" },
          { "direction": "in",  "text": "А умножить на 3?" }
      ] }
      """

  Scenario: Ouro cannot answer a thread that does not exist
    When I call "reply_to_peer" with:
      """
      { "threadId": 999, "text": "кому?" }
      """
    Then the call fails with "Unknown thread 999"

  Scenario: A peer cannot continue a thread that does not exist
    When the peer calls "ask_ouro" with:
      """
      { "threadId": 999, "text": "алло" }
      """
    Then the call fails with "Unknown thread 999"
    And 0 signals are waiting

  Scenario: An empty message is refused
    When the peer calls "ask_ouro" with:
      """
      { "text": "   " }
      """
    Then the call is rejected as invalid parameters
    And 0 signals are waiting

  Scenario: The peer endpoint offers only the conversation
    Then the peer endpoint offers exactly these tools:
      """
      ask_ouro read_replies
      """
