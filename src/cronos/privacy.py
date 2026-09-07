"""Explicit deletion requests; execution is fenced separately from model tool calls."""

from uuid import UUID

from cronos.file_tasks import file_task

FULL_CONFIRMATION = "подтверждаю полную очистку"
CHAT_CONFIRMATION = "подтверждаю удаление чата"


def confirmation_scope(text: str) -> str | None:
    normalized = text.strip().casefold().rstrip(".! ")
    return {FULL_CONFIRMATION: "all", CHAT_CONFIRMATION: "chat"}.get(normalized)


def confirmation_text(request: dict) -> str:
    if request["scope"] == "all":
        return (
            "Полная очистка удалит память, настройки, файлы, все чаты и переписку Cronos, "
            "а также все задания по расписанию. Тариф, баланс и учтённые расходы сохранятся.\n\n"
            "Темы Telegram удалятся вместе с историей. Старые сообщения общего чата "
            "старше 48 часов Telegram не разрешает удалять боту — их можно очистить вручную.\n\n"
            "Отменить удаление нельзя. Нажми «Удалить всё» или напиши: "
            "«Подтверждаю полную очистку». Подтверждение действует 15 минут."
        )
    title = request.get("title") or "этот чат"
    general = request.get("thread_id", 0) in {0, 1}
    action = (
        "История общего чата будет удалена из Cronos вместе с его заданиями. "
        "В Telegram сам общий чат останется; сообщения старше 48 часов нужно очистить вручную."
        if general
        else f"Чат «{title}» будет удалён вместе с перепиской и заданиями этого чата."
    )
    return (
        action + "\n\nОбщая память, библиотека файлов, другие чаты и тариф сохранятся. "
        "Для удаления всей персональной информации используй /clearall.\n\n"
        "Отменить удаление нельзя. Нажми «Удалить чат» или напиши: "
        "«Подтверждаю удаление чата». Подтверждение действует 15 минут."
    )


def confirmation_keyboard(request: dict) -> dict:
    token = UUID(str(request["id"])).hex
    return {
        "inline_keyboard": [
            [
                {
                    "text": "Удалить всё" if request["scope"] == "all" else "Удалить чат",
                    "callback_data": f"privacy:confirm:{token}",
                }
            ],
            [{"text": "Отмена", "callback_data": f"privacy:cancel:{token}"}],
        ]
    }


async def perform_erasure(store, transport, artifacts, event: dict):
    """Called for a durable privacy event, never directly by the language model.

    Lock order is work then delivery. File writers keep the work lock until their
    underlying thread finishes; senders re-read outbox while holding delivery.
    """
    user_id = event["payload"]["user_id"]
    request_id = event["payload"]["request_id"]
    async with store.user_lock(user_id), store.user_lock(user_id, purpose="delivery"):
        request = await store.begin_privacy_erasure(request_id, event["id"])
        if request is None:
            return
        if request["user_id"] != user_id:
            raise ValueError("Privacy request owner does not match its event")
        job = request["job"]
        last_attempt = event.get("attempts", 1) >= 3
        files_erased, files_failed = False, False
        if job.get("delete_user_files"):
            try:
                await file_task(artifacts.erase_user, user_id)
                files_erased = True
            except Exception:
                if not last_attempt:
                    raise
                files_failed = True
        failed_topics = []
        cleared_topics = 0
        for thread_id in job.get("topic_ids", []):
            try:
                if not await transport.delete_topic(request["chat_id"], thread_id):
                    raise RuntimeError("Telegram did not acknowledge topic deletion")
                cleared_topics += 1
            except Exception:
                if not last_attempt:
                    raise
                failed_topics.append(thread_id)
        messages = {"cleared": 0, "failed": 0, "unavailable_ids": []}
        if job.get("general_message_ids"):
            try:
                messages = await transport.delete_messages(
                    request["chat_id"], job["general_message_ids"]
                )
            except Exception:
                if not last_attempt:
                    raise
                messages["failed"] = len(job["general_message_ids"])
        await store.finish_privacy_erasure(
            request_id,
            {
                "database_erased": True,
                "files_erased": files_erased,
                "files_failed": files_failed,
                "topics_cleared": cleared_topics,
                "topics_failed": len(failed_topics),
                "messages_cleared": messages["cleared"],
                "messages_failed": messages["failed"],
                "general_history_limited": bool(job.get("general_history_limited")),
                "billing_preserved": True,
            },
        )
