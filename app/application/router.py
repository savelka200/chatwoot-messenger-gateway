import logging
from typing import Any, Dict, Optional

from app.domain.message import MediaContent, TextContent
from app.domain.ports import MessengerAdapter
from app.domain.webhooks.chatwoot import ChatwootMessageCreatedWebhook

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

    async def handle_outgoing(self, payload: dict) -> None:
        """
        Process Chatwoot outgoing webhook and dispatch text/media to a proper adapter.
        Note: we trust channel injected at HTTP layer: payload['conversation']['meta']['channel'].
        """
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

        # Channel comes from raw payload (HTTP layer injected it into meta)
        channel = _dig(payload, "conversation", "meta", "channel")
        text = (cw.content or "").strip()

        # Always derive recipient_id (Chatwoot never provides it)
        recipient_id = self._derive_recipient_id(channel=channel, payload=payload)

        if not channel or not recipient_id:
            logger.warning(
                "[router] Missing fields: channel=%r recipient_id=%r",
                channel,
                recipient_id,
            )
            return

        # Check for attachments first
        attachments = cw.attachments or []
        if attachments:
            # Send media attachments
            for att in attachments:
                await self.dispatch_media(
                    channel=channel,
                    recipient_id=recipient_id,
                    attachment=att,
                    caption=text if text else None,
                )
        elif text:
            # Send text only if no attachments
            await self.dispatch_outbound(
                channel=channel, recipient_id=recipient_id, text=text
            )

    async def dispatch_outbound(
        self, channel: str, recipient_id: str, text: str
    ) -> None:
        """Send text via selected channel adapter."""
        adapter = self.adapters.get(channel)
        if not adapter:
            logger.warning("[router] No adapter for channel=%s", channel)
            return

        await adapter.send_text(recipient_id, TextContent(type="text", text=text))
        logger.info(
            "[router] OUTBOUND: channel=%s recipient_id=%s text=%r",
            channel,
            recipient_id,
            text,
        )

    async def dispatch_media(
        self, channel: str, recipient_id: str, attachment: Any, caption: Optional[str] = None
    ) -> None:
        """Send media attachment via selected channel adapter."""
        adapter = self.adapters.get(channel)
        if not adapter:
            logger.warning("[router] No adapter for channel=%s", channel)
            return

        # Map content_type to media_type
        content_type = attachment.content_type or ""
        filename = attachment.filename or ""
        
        media_type_map = {
            "image/": "image",
            "video/": "video",
            "audio/": "audio",
        }
        
        media_type = "document"  # default
        for prefix, mtype in media_type_map.items():
            if content_type.startswith(prefix):
                media_type = mtype
                break
        
        # Use data_url or file_url for download
        url = attachment.data_url or attachment.file_url
        if not url:
            logger.warning("[router] No URL in attachment: %s", attachment)
            return

        media_content = MediaContent(
            type="media",
            media_type=media_type,  # type: ignore
            url=url,
            caption=caption,
            filename=filename if filename else None,
            mime_type=content_type if content_type else None,
        )

        await adapter.send_media(recipient_id, media_content)
        logger.info(
            "[router] OUTBOUND MEDIA: channel=%s recipient_id=%s type=%s url=%s",
            channel,
            recipient_id,
            media_type,
            url,
        )
