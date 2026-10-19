---
tools: [list_nashdom_mails, download_gmail_attachment, read_pdf, send_telegram_message]
---

# NashDom — стиль уведомления

For a user asking whether bills arrived, list subjects, dates and senders first.
Do not claim payment is due until the PDF has been read: a bill can have zero
balance. Process or extract the PDFs when requested, rather than treating a
listing question as authorization for the whole automatic bill workflow.


Сигнал `source=nashdom-bill` уже самодостаточен: в `content` лежит инструкция,
все нужные id (messageId, attachmentId) и сама команда что делать. Выполняй
по шагам из сигнала. По умолчанию верни родителю готовый текст уведомления: он отвечает за
отправку. Если родитель явно поручил отправку и дал адресата, отправь сам
и верни короткий отчёт. Этот файл задаёт **формат** уведомления.

## Формат для квитанции (PDF был, ты его прочитал)

```
📄 Нова квитанція NashDom
<тип, напр. "Квитанція №1 (ЖКП)"> · <період YYYY-MM>
<1–4 категории, через "·" — за что счёт>
<2–5 самых полезных строк: "Холодна вода: 12 м³", "Опалення: 144.14 UAH">

Всього: <сумма> UAH
```

- ≤ ~12 строк, plain text, без Markdown.
- Если PDF разобрался плохо — заголовок + итог достаточно. Не выдумывай
  цифры; если поле не извлеклось, так и напиши.

## Формат для письма без вложения

```
✉️ NashDom
<subject>

<snippet — обрежь если длинный>
```

## Правила

- **Одно сообщение на одно письмо.** Не дроби.
- **Не помечай ничего как оплаченное** — этим занимается reconcile-bill-payments.
- **Старые периоды (до 2026-05).** Платёжный трекинг начался с мая 2026, но
  уведомление о приходе квитанции всё равно отправляй. Не предлагай оплачивать
  старые счета — просто констатация ("пришла квитанция за <период>").
- **Верни полный результат, а не только ключ памяти.** Runtime сохранит его
  автоматически. При больших ответах `read_pdf` прочитай полный текст через
  `working_memory_get`; не извлекай суммы только из preview.
