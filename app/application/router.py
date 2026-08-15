import logging
from typing import Any, Dict, List, Optional

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

    def _parse_chatwoot_attachments(
            self, raw_attachments: List[Dict[str, Any]]
        ) -> List[MediaContent]:
            """
            Преобразует массив attachments из webhook Chatwoot в список MediaContent.
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
                    # Попытка определить расширение из MIME
                    guessed_ext = mimetypes.guess_extension(mime) or ""
                    extension = guessed_ext.lstrip(".") or default_ext

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
                logger.info("[router] Ignored Chatwoot event: %s", cw.event)
                return
            if cw.private:
                logger.info("[router] Ignored private message")
                return
            if cw.message_type != "outgoing":
                logger.info("[router] Ignored message_type: %s", cw.message_type)
                return
    
            channel = _dig(payload, "conversation", "meta", "channel")
            text = (cw.content or "").strip()
            recipient_id = self._derive_recipient_id(channel=channel, payload=payload)
    
            # === НОВОЕ: парсим вложения ===
            raw_attachments = _dig(payload, "attachments", default=[]) or []
            attachments = self._parse_chatwoot_attachments(raw_attachments)
    
            # === ИЗМЕНЕНО: разрешаем пустой текст, если есть вложения ===
            if not channel or not recipient_id:
                logger.warning(
                    "[router] Missing fields: channel=%r recipient_id=%r",
                    channel, recipient_id,
                )
                return
            if not text and not attachments:
                logger.info("[router] Empty message without attachments — ignored")
                return
    
            # === НОВОЕ: поддержка reply (пока без реализации маппинга ID) ===
            reply_to_message_id = _dig(payload, "in_reply_to", "id")  # если есть
            reply_to_message_id = str(reply_to_message_id) if reply_to_message_id else None
    
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
            )
    
    async def dispatch_outbound(
        self,
        channel: str,
        recipient_id: str,
        text: str,
        attachments: List[MediaContent],
        reply_to_message_id: Optional[str] = None,
    ) -> None:
        adapter = self.adapters.get(channel)
        if not adapter:
            logger.warning("[router] No adapter for channel=%s", channel)
            return

        # Если адаптер поддерживает новый метод send_message — используем его
        if hasattr(adapter, "send_message"):
            await adapter.send_message(
                recipient_id=recipient_id,
                text=text,
                attachments=attachments,
                reply_to_message_id=reply_to_message_id,
            )
        else:
            # Fallback — только текст (для адаптеров, которые ещё не обновили)
            if text:
                await adapter.send_text(recipient_id, TextContent(type="text", text=text))
            else:
                logger.warning(
                    "[router] Adapter %s has no send_message and message has no text — skipped",
                    channel,
                )