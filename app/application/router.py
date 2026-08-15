import logging
from typing import Any, Dict, List, Optional
from urllib.parse import unquote, urlparse
import mimetypes

from app.domain.message import TextContent
from app.domain.ports import MessengerAdapter
from app.domain.webhooks.chatwoot import ChatwootMessageCreatedWebhook
from app.domain.message import MediaContent, TextContent

logger = logging.getLogger(__name__)


def _dig(src: dict, *path, default=None):
    """Safe dict traversal: _dig(d, 'a','b','c') -> d['a']['b']['c'] or default."""
    cur: Any = src
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


class MessageRouter:
    """Router: dispatch outgoing text messages to channel adapters."""

    def __init__(self, adapters: Dict[str, MessengerAdapter] | None = None):
        self.adapters = adapters or {}

    async def handle_incoming(self, msg):
        # Not implemented in this demo
        logger.info(
            "[router] INCOMING: channel=%s recipient_id=%s sender_name=%s content=%s",
            getattr(msg, "channel", None),
            getattr(msg, "recipient_id", None),
            getattr(msg, "sender_name", None),
            getattr(msg, "content", None),
        )

    def _derive_recipient_id(self, channel: str | None, payload: dict) -> str | None:
        """
        Build recipient_id per channel. We never read it from Chatwoot.
        whatsapp:
          - conversation.meta.sender.phone_number
        telegram:
          1) sender.custom_attributes.telegram_username                  -> '@username' or 'username'
          2) sender.additional_attributes.social_telegram_user_name      -> '@username'
          3) sender.phone_number                                         -> '+7999...'
          4) sender.custom_attributes.telegram_user_id                   -> 'id:<int>'
          5) sender.additional_attributes.social_telegram_user_id        -> 'id:<int>'
        vk:
          1) sender.custom_attributes.vk_peer_id                         -> '<int>'
          2) sender.custom_attributes.vk_user_id                         -> '<int>'
        """
        if not channel:
            return None

        sender = _dig(payload, "conversation", "meta", "sender", default={}) or {}

        if channel == "whatsapp":
            phone = (sender.get("phone_number") or "").strip()
            return phone or None

        if channel == "telegram":
            # 1) username from custom attributes
            username = (sender.get("custom_attributes", {}) or {}).get(
                "telegram_username", ""
            )
            username = (username or "").strip()
            if username:
                return username

            # 2) username from additional attributes (added by Chatwoot TG bot)
            social_username = (sender.get("additional_attributes", {}) or {}).get(
                "social_telegram_user_name", ""
            )
            social_username = (social_username or "").strip()
            if social_username:
                return social_username

            # 3) phone number
            phone = (sender.get("phone_number") or "").strip()
            if phone:
                return phone

            # 4) numeric user id from custom attributes
            tg_uid = (sender.get("custom_attributes", {}) or {}).get("telegram_user_id")
            if tg_uid is not None and str(tg_uid).strip():
                return f"id:{tg_uid}"

            # 5) numeric user id from additional attributes (added by Chatwoot TG bot)
            social_tg_uid = (sender.get("additional_attributes", {}) or {}).get(
                "social_telegram_user_id"
            )
            if social_tg_uid is not None and str(social_tg_uid).strip():
                return f"id:{social_tg_uid}"

            return None

        if channel == "vk":
            # 1) peer_id from custom attributes
            vk_peer_id = (sender.get("custom_attributes", {}) or {}).get("vk_peer_id")
            if vk_peer_id is not None and str(vk_peer_id).strip():
                return str(vk_peer_id).strip()

            # 2) user_id from custom attributes
            vk_user_id = (sender.get("custom_attributes", {}) or {}).get("vk_user_id")
            if vk_user_id is not None and str(vk_user_id).strip():
                return str(vk_user_id).strip()

            return None

        # Other channels: do not guess
        return None

    def _extract_filename_from_url(self, data_url: str) -> Optional[str]:
        """
        Извлекает оригинальное имя файла из URL Chatwoot.
        Пример: https://.../blobs/redirect/<hash>/Практика1.docx → "Практика1.docx"
        """
        try:
            parsed = urlparse(data_url)
            path = parsed.path
            # Берём последний сегмент пути
            filename = path.split("/")[-1] if path else None
            if filename:
                # Декодируем URL-encoded символы (%D0%9F... → кириллица)
                return unquote(filename)
        except Exception as e:
            logger.warning("[router] Failed to extract filename from URL: %s", e)
        return None

    def _parse_chatwoot_attachments(
        self, raw_attachments: List[Dict[str, Any]]
    ) -> List[MediaContent]:
        """
        Преобразует массив attachments из webhook Chatwoot в список MediaContent.
        Извлекает оригинальные имена файлов из data_url.
        """
        result: List[MediaContent] = []
        for idx, att in enumerate(raw_attachments or []):
            data_url = att.get("data_url")
            if not data_url:
                continue

            file_type = (att.get("file_type") or "").lower()
            extension = (att.get("extension") or "").strip().lstrip(".")
            content_type = (att.get("content_type") or "").split(";")[0] or None
            file_size = att.get("file_size")

            # Маппинг file_type Chatwoot → media_type MediaContent
            if file_type == "image":
                media_type = "image"
                default_mime = "image/jpeg"
                default_ext = "jpg"
            elif file_type == "video":
                media_type = "video"
                default_mime = "video/mp4"
                default_ext = "mp4"
            elif file_type == "audio":
                media_type = "audio"
                default_mime = "audio/mpeg"
                default_ext = "mp3"
            elif file_type == "file":
                media_type = "document"
                default_mime = "application/octet-stream"
                default_ext = "bin"
            else:
                media_type = "document"
                default_mime = "application/octet-stream"
                default_ext = "bin"

            mime = content_type or default_mime
            if not extension:
                guessed_ext = mimetypes.guess_extension(mime) or ""
                extension = guessed_ext.lstrip(".") or default_ext

            # === НОВОЕ: извлекаем оригинальное имя из URL ===
            original_filename = self._extract_filename_from_url(data_url)
            if original_filename:
                filename = original_filename
            else:
                # Fallback: генерируем имя
                filename = f"chatwoot_{att.get('id') or idx}.{extension}"

            result.append(
                MediaContent(
                    type="media",
                    media_type=media_type,
                    url=data_url,
                    caption=None,
                    filename=filename,
                    mime_type=mime,
                    raw={"size": file_size} if file_size else {},
                )
            )
        return result


    async def handle_outgoing(self, payload: dict) -> None:
        try:
            cw = ChatwootMessageCreatedWebhook.model_validate(payload)
        except Exception as e:
            logger.warning("[router] Invalid Chatwoot payload: %s", e)
            return

        if cw.event != "message_created":
            return
        if cw.private:
            return
        if cw.message_type != "outgoing":
            return

        channel = _dig(payload, "conversation", "meta", "channel")
        text = (cw.content or "").strip()
        recipient_id = self._derive_recipient_id(channel=channel, payload=payload)

        raw_attachments = _dig(payload, "attachments", default=[]) or []
        attachments = self._parse_chatwoot_attachments(raw_attachments)

        if not channel or not recipient_id:
            logger.warning("[router] Missing fields: channel=%r recipient_id=%r", channel, recipient_id)
            return
        if not text and not attachments:
            return

        reply_to_message_id = _dig(payload, "in_reply_to", "id")
        reply_to_message_id = str(reply_to_message_id) if reply_to_message_id else None

        # === НОВОЕ: извлекаем conversation_id ===
        conversation_id = _dig(payload, "conversation", "id")

        logger.info(
            "[router] OUTGOING: channel=%s recipient=%s text=%r attachments=%d reply=%s",
            channel, recipient_id, text[:50] if text else "", len(attachments), reply_to_message_id,
        )

        await self.dispatch_outbound(
            channel=channel,
            recipient_id=recipient_id,
            text=text,
            attachments=attachments,
            reply_to_message_id=reply_to_message_id,
            conversation_id=conversation_id,
        )
    
    async def dispatch_outbound(
        self,
        channel: str,
        recipient_id: str,
        text: str,
        attachments: List[MediaContent],
        reply_to_message_id: Optional[str] = None,
        conversation_id: Optional[int] = None,  # НОВОЕ: для отправки уведомления
    ) -> None:
        adapter = self.adapters.get(channel)
        if not adapter:
            logger.warning("[router] No adapter for channel=%s", channel)
            return

        failed_attachments: List[str] = []

        if hasattr(adapter, "send_message"):
            failed_attachments = await adapter.send_message(
                recipient_id=recipient_id,
                text=text,
                attachments=attachments,
                reply_to_message_id=reply_to_message_id,
            )
        else:
            if text:
                await adapter.send_text(recipient_id, TextContent(type="text", text=text))

        # === НОВОЕ: отправляем уведомление оператору в Chatwoot ===
        if failed_attachments and conversation_id:
            await self._notify_operator_about_failed_attachments(
                conversation_id=conversation_id,
                failed_attachments=failed_attachments,
            )

    async def _notify_operator_about_failed_attachments(
        self,
        conversation_id: int,
        failed_attachments: List[str],
    ) -> None:
        """
        Отправляет уведомление оператору в Chatwoot о том,
        что некоторые вложения не удалось отправить в ВК.
        """
        try:
            # Импортируем здесь, чтобы избежать circular import
            from app.application.chatwoot_service import ChatwootService
            from app.infra.chatwoot_client import ChatwootClient
            from app.config import load_config

            config = load_config()
            cw_client = ChatwootClient(
                api_access_token=config.chatwoot.api_access_token,
                account_id=config.chatwoot.account_id,
                base_url=str(config.chatwoot.base_url),
            )
            cw = ChatwootService(client=cw_client)

            error_text = "⚠️ Не удалось отправить некоторые вложения в ВК:\n"
            error_text += "\n".join(failed_attachments)

            await cw.create_message(
                conversation_id=conversation_id,
                content=error_text,
                direction="outgoing",
                private=True,
            )
            logger.warning(
                "[router] Notified operator about failed attachments: conversation_id=%s count=%d",
                conversation_id, len(failed_attachments),
            )
        except Exception as e:
            logger.error("[router] Failed to notify operator about failed attachments: %s", e)