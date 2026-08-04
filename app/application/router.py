import logging
from typing import Any, Dict, Optional

from app.application.chatwoot_service import ChatwootService
from app.domain.message import (
    ContactContent,
    LocationContent,
    MediaContent,
    StickerContent,
    TextContent,
)
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
    """Router: dispatch outgoing messages to channel adapters."""

    def __init__(self, adapters: Dict[str, MessengerAdapter] | None = None, cw_service: Optional[ChatwootService] = None):
        self.adapters = adapters or {}
        self._cw_service = cw_service

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
        Process Chatwoot outgoing webhook and dispatch to a proper adapter.
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

        # Always derive recipient_id (Chatwoot never provides it)
        recipient_id = self._derive_recipient_id(channel=channel, payload=payload)

        # Check for attachments first (media, sticker, etc.)
        # Chatwoot can send attachments in root or in message object
        attachments = _dig(payload, "attachments", default=[]) or _dig(payload, "message", "attachments", default=[])
        if attachments and len(attachments) > 0:
            if not channel:
                logger.warning(
                    "[router] Missing channel for attachment",
                )
                return
            # Derive recipient_id if not already derived
            if not recipient_id:
                recipient_id = self._derive_recipient_id(channel=channel, payload=payload)
            if not recipient_id:
                logger.warning(
                    "[router] Missing recipient_id for attachment: channel=%r",
                    channel,
                )
                return
            await self.dispatch_outbound_with_attachments(
                channel=channel,
                recipient_id=recipient_id,
                attachments=attachments,
                text=cw.content or "",
            )
            return

        text = (cw.content or "").strip()
        if not channel or not recipient_id or not text:
            logger.warning(
                "[router] Missing fields: channel=%r recipient_id=%r text=%r",
                channel,
                recipient_id,
                text,
            )
            return

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

    async def dispatch_outbound_with_attachments(
        self,
        channel: str,
        recipient_id: str | None,
        attachments: list,
        text: str,
    ) -> None:
        """Send media/sticker/location/contact via selected channel adapter."""
        if not recipient_id:
            logger.warning(
                "[router] Cannot send attachment without recipient_id: channel=%s",
                channel,
            )
            return

        adapter = self.adapters.get(channel)
        if not adapter:
            logger.warning("[router] No adapter for channel=%s", channel)
            return

        # Process each attachment
        for att in attachments:
            att_type = att.get("type")
            data_url = att.get("data_url") or att.get("url")

            try:
                if att_type in ("image", "video", "audio", "file"):
                    # Map to MediaContent
                    media_type_map = {
                        "image": "image",
                        "video": "video",
                        "audio": "audio",
                        "file": "document",
                    }
                    media_type = media_type_map.get(att_type, "document")
                    content = MediaContent(
                        type="media",
                        media_type=media_type,  # type: ignore
                        url=data_url,
                        caption=text,
                        filename=att.get("filename"),
                        mime_type=att.get("mime_type"),
                    )
                    await adapter.send_media(recipient_id, content)
                    logger.info(
                        "[router] OUTBOUND media: channel=%s type=%s",
                        channel,
                        media_type,
                    )

                elif att_type == "sticker":
                    # Sticker with image URL
                    content = StickerContent(type="sticker", ref=data_url or "")
                    await adapter.send_sticker(recipient_id, content)
                    logger.info("[router] OUTBOUND sticker: channel=%s", channel)

                elif att_type == "location":
                    # Location with lat/long
                    lat = att.get("latitude", 0.0)
                    lon = att.get("longitude", 0.0)
                    name = att.get("name") or text
                    content = LocationContent(
                        type="location",
                        latitude=lat,
                        longitude=lon,
                        name=name,
                    )
                    await adapter.send_location(recipient_id, content)
                    logger.info(
                        "[router] OUTBOUND location: channel=%s lat=%s long=%s",
                        channel,
                        lat,
                        lon,
                    )

                elif att_type == "contact":
                    # Contact card
                    name = att.get("name", "Unknown")
                    phone = att.get("phone_number", "")
                    org = att.get("org")
                    content = ContactContent(
                        type="contact",
                        name=name,
                        phone=phone,
                        org=org,
                    )
                    await adapter.send_contact(recipient_id, content)
                    logger.info(
                        "[router] OUTBOUND contact: channel=%s name=%s",
                        channel,
                        name,
                    )

                else:
                    logger.warning(
                        "[router] Unknown attachment type: %s, sending as text",
                        att_type,
                    )
                    if text:
                        await self.dispatch_outbound(channel, recipient_id, text)

            except Exception as e:
                logger.exception(
                    "[router] Failed to send attachment %s: %s", att_type, e
                )
