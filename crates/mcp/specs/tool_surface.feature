Feature: The tools the agent sees
  The supervisor's server offers exactly this set of tools. Anything added or
  removed is a decision, so it shows up here as a failing line.

  Scenario: The full instance's tool list
    Then the server offers exactly these tools:
      """
      add_note append_doc create_project doc_history
      download_gmail_attachment edit_telegram_message fetch_article fetch_url
      find_notes get_fact get_next_signal get_peer_thread get_telegram_chat_history get_timezone
      journal_append journal_tail list_memory list_monobank_transactions list_nashdom_mails list_news
      list_scheduled_tasks list_signals list_userbot_dialogs patch_doc read_doc
      read_file read_pdf recall remember reply_to_peer revert_patch search_news
      send_telegram_chat_action send_telegram_message set_timezone start_typing
      telegram_send_status update_fact wait_for_signal write_doc
      """

  Scenario: Sending to Telegram without a configured chat explains what is missing
    When I call "send_telegram_message" with:
      """
      { "text": "hello" }
      """
    Then the call fails with "No chat target"

  Scenario: Telegram buttons validate callback byte limits before sending
    When I call "send_telegram_message" with:
      """
      { "text": "Choose", "buttons": [[{ "text": "Confirm", "data": "" }]] }
      """
    Then the call is rejected as invalid parameters
