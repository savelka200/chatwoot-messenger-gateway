import logging
import secrets
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

import httpx
from pyee.asyncio import AsyncIOEventEmitter

from app.config import VKCommunityConfig
from app.domain.message import (
    ContactContent,
    LocationContent,
    MediaContent,
    StickerContent,
    TextContent,
    UnifiedMessage,
)
from app.domain.ports import MessengerAdapter, OnMessage

logger = logging.getLogger(__name__)


class VkAdapter(MessengerAdapter):
    """VK adapter for Callback API with media support."""

    def __init__(self, bus: AsyncIOEventEmitter, config: VKCommunityConfig):
        self._bus = bus
        self._config = config
        self.inbox_id = config.inbox_id
        self._cb: Optional[OnMessage] = None
        self._incoming_listener: Optional[Callable[..., Awaitable[None]]] = None
        self._confirm_listener: Optional[Callable[..., Awaitable[None]]] = None
        self._http: Optional[httpx.AsyncClient] = None

    def on_message(self, cb: OnMessage) -> None:
        self._cb = cb

    def confirmation_token(self) -> str:
        return self._config.confirmation

    def capabilities(self) -> Set[str]:
        return {"text", "media", "sticker", "contact", "location"}

    async def start(self) -> None:
        # Initialize HTTP client once
        if self._http is None:
            self._http = httpx.AsyncClient(
                base_url="https://api.vk.ru/method",
                timeout=15,
                headers={"User-Agent": "chatwoot-integration/1.0"},
            )

        async def _on_vk_incoming(payload: Dict[str, Any]) -> None:
            if payload.get("event") != "message_new" or not self._cb:
                return

            msg = payload.get("message") or {}
            text = (msg.get("text") or "").strip()
            peer_id = str(msg.get("peer_id")) if msg.get("peer_id") is not None else ""
            from_id = (
                str(msg.get("from_id")) if msg.get("from_id") is not None else peer_id
            )
            message_id = str(msg.get("id")) if msg.get("id") is not None else None

            if not self._cb or not peer_id:
                logger.debug("[vk] skip incoming: no callback or missing peer_id")
                return

            # Parse attachments if present
            attachments = msg.get("attachments", [])
            content = self._parse_attachments(attachments, text)

            umsg = UnifiedMessage(
                channel="vk",
                sender_id=from_id,
                recipient_id=peer_id,
                message_id=message_id,
                content=content,
                raw=payload,
            )
            await self._cb(umsg)

        async def _on_vk_confirmation(payload: Dict[str, Any]) -> None:
            group_id = payload.get("group_id")
            logger.info("[vk] confirmation request received for group_id=%s", group_id)

        self._incoming_listener = _on_vk_incoming
        self._confirm_listener = _on_vk_confirmation
        self._bus.on("vk.incoming", self._incoming_listener)
        self._bus.on("vk.confirmation", self._confirm_listener)

        logger.info("[vk] adapter started (callback API with media support)")

    def _parse_attachments(
        self, attachments: List[Dict[str, Any]], text: str
    ) -> "Content":
        """Parse VK attachments into UnifiedMessage content."""
        if not attachments:
            return TextContent(type="text", text=text)

        # For now, handle the first attachment only
        # VK can send multiple attachments, but we'll process the first media item
        att = attachments[0]
        att_type = att.get("type")

        if att_type == "photo":
            photo = att.get("photo", {})
            # Get the highest resolution URL available
            url = photo.get("url") or photo.get("sizes", [{}])[-1].get("url", "")
            caption = text if text else photo.get("text")
            return MediaContent(
                type="media",
                media_type="image",
                url=url,
                caption=caption,
                filename=None,
                mime_type="image/jpeg",
            )

        elif att_type == "video":
            video = att.get("video", {})
            url = video.get("player", "") or video.get("url", "")
            caption = text if text else video.get("description")
            return MediaContent(
                type="media",
                media_type="video",
                url=url,
                caption=caption,
                filename=None,
                mime_type="video/mp4",
            )

        elif att_type == "audio_message":
            audio = att.get("audio_message", {})
            url = audio.get("link_ogg", "") or audio.get("link_mp3", "")
            return MediaContent(
                type="media",
                media_type="audio",
                url=url,
                caption=text,
                filename=audio.get("title"),
                mime_type="audio/ogg",
            )

        elif att_type == "doc":
            doc = att.get("doc", {})
            url = doc.get("url", "")
            return MediaContent(
                type="media",
                media_type="document",
                url=url,
                caption=text,
                filename=doc.get("title"),
                mime_type=doc.get("mime_type"),
            )

        elif att_type == "sticker":
            sticker = att.get("sticker", {})
            # VK stickers have photo URLs in different sizes
            url = (
                sticker.get("photo_256")
                or sticker.get("photo_128")
                or sticker.get("photo_64")
                or ""
            )
            return StickerContent(type="sticker", ref=url)

        elif att_type == "geo":
            geo = att.get("geo", {})
            coordinates = geo.get("coordinates", {})
            lat = coordinates.get("latitude", 0.0)
            lon = coordinates.get("longitude", 0.0)
            place_name = geo.get("place", {}).get("title") if geo.get("place") else None
            return LocationContent(
                type="location",
                latitude=lat,
                longitude=lon,
                name=place_name,
            )

        # Fallback to text if attachment type is not recognized
        return TextContent(type="text", text=text)

    async def stop(self) -> None:
        if self._incoming_listener:
            try:
                self._bus.remove_listener("vk.incoming", self._incoming_listener)  # type: ignore[attr-defined]
            except Exception:
                pass
            self._incoming_listener = None

        if self._confirm_listener:
            try:
                self._bus.remove_listener("vk.confirmation", self._confirm_listener)  # type: ignore[attr-defined]
            except Exception:
                pass
            self._confirm_listener = None

        # Close HTTP client
        if self._http:
            try:
                await self._http.aclose()
            except Exception:
                pass
            self._http = None

        logger.info("[vk] adapter stopped")

    async def _vk_call(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """VK API call with basic error handling."""
        if not self._http:
            raise RuntimeError("VK HTTP client is not initialized")

        # Required parameters
        params = {
            **params,
            "access_token": self._config.access_token,
            "v": self._config.api_version,
        }

        resp = await self._http.post(f"/{method}", data=params)
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            err = data["error"]
            code = err.get("error_code")
            msg = err.get("error_msg")
            logger.error("[vk] API error %s: %s; params=%s", code, msg, params)
            raise RuntimeError(f"VK API error {code}: {msg}")

        return data.get("response", data)

    async def send_text(self, recipient_id: str, content: TextContent) -> None:
        """Send a text message via VK messages.send."""
        # recipient_id must be peer_id: user_id, chat peer (2e9+chat_id) or group peer
        text = content.text or ""
        if not text:
            logger.info("[vk] skip send: empty text")
            return

        try:
            random_id = secrets.randbits(31)  # unique random_id per request
            params = {
                "peer_id": int(recipient_id),
                "message": text,
                "random_id": random_id,
                # For a community token, group_id can be omitted, but doesn't hurt
                "group_id": self._config.group_id,
                # Optionally, disable_mentions=1 can be added if needed
            }
            res = await self._vk_call("messages.send", params)
            # Success: VK returns message ID or an array
            logger.info("[vk] SENT: peer_id=%s message_id=%s", recipient_id, res)
        except Exception as e:
            logger.exception("[vk] Failed to send text to %s: %s", recipient_id, e)

    async def send_media(self, recipient_id: str, content: MediaContent) -> None:
        """Send media (photo, video, audio, document) via VK messages.send."""
        url = str(content.url)
        caption = content.caption or ""

        try:
            random_id = secrets.randbits(31)
            params = {
                "peer_id": int(recipient_id),
                "random_id": random_id,
                "group_id": self._config.group_id,
            }

            # Determine attachment type and upload method
            if content.media_type == "image":
                # For photos, we need to upload first using photos.getMessagesUploadServer
                photo_params = {"peer_id": int(recipient_id)}
                upload_resp = await self._vk_call(
                    "photos.getMessagesUploadServer", photo_params
                )
                upload_url = upload_resp.get("upload_url")
                if not upload_url:
                    logger.error("[vk] No upload URL received for photo")
                    return

                # Upload photo to VK server
                async with httpx.AsyncClient() as client:
                    # Download the image from URL
                    img_resp = await client.get(url)
                    img_resp.raise_for_status()
                    
                    # Upload to VK
                    files = {"photo": ("image.jpg", img_resp.content, "image/jpeg")}
                    upload_result = await client.post(upload_url, files=files)
                    upload_result.raise_for_status()
                    upload_data = upload_result.json()

                # Save the photo
                photo_id = upload_data.get("photo")
                if not photo_id:
                    logger.error("[vk] No photo ID received after upload")
                    return

                save_params = {
                    "photo": photo_id,
                    "server": upload_data.get("server"),
                    "hash": upload_data.get("hash"),
                }
                saved_photo = await self._vk_call("photos.saveMessagesPhoto", save_params)
                
                if saved_photo and len(saved_photo) > 0:
                    photo_obj = saved_photo[0]
                    attachment_str = f"photo{photo_obj['owner_id']}_{photo_obj['id']}"
                    params["attachment"] = attachment_str
                    if caption:
                        params["message"] = caption

            elif content.media_type == "video":
                # For videos, use messages.send with link or upload
                # Simple approach: send as link with caption
                params["message"] = f"{caption}\n{url}" if caption else url

            elif content.media_type == "audio":
                # Audio messages require special upload flow
                # For now, send as document or link
                params["message"] = f"{caption}\n{url}" if caption else url

            elif content.media_type == "document":
                # Upload document using docs.getMessagesUploadServer
                doc_params = {"type": "doc"}
                upload_resp = await self._vk_call(
                    "docs.getMessagesUploadServer", doc_params
                )
                upload_url = upload_resp.get("upload_url")
                if not upload_url:
                    logger.error("[vk] No upload URL received for document")
                    return

                # Download and upload document
                async with httpx.AsyncClient() as client:
                    doc_resp = await client.get(url)
                    doc_resp.raise_for_status()
                    
                    filename = content.filename or "document"
                    mime_type = content.mime_type or "application/octet-stream"
                    files = {"file": (filename, doc_resp.content, mime_type)}
                    upload_result = await client.post(upload_url, files=files)
                    upload_result.raise_for_status()
                    upload_data = upload_result.json()

                # Save the document
                save_params = {
                    "file": upload_data.get("file"),
                }
                saved_doc = await self._vk_call("docs.save", save_params)
                
                if saved_doc and len(saved_doc) > 0:
                    doc_obj = saved_doc[0]
                    attachment_str = f"doc{doc_obj['owner_id']}_{doc_obj['id']}"
                    params["attachment"] = attachment_str
                    if caption:
                        params["message"] = caption

            res = await self._vk_call("messages.send", params)
            logger.info("[vk] SENT media: peer_id=%s result=%s", recipient_id, res)
        except Exception as e:
            logger.exception("[vk] Failed to send media to %s: %s", recipient_id, e)

    async def send_sticker(self, recipient_id: str, content: StickerContent) -> None:
        """Send a sticker via VK messages.send."""
        # Stickers in VK require sticker_id, not URL
        # The ref field should contain sticker_id
        sticker_id = content.ref
        if not sticker_id or not sticker_id.isdigit():
            logger.warning("[vk] Invalid sticker ref (expected numeric id): %s", sticker_id)
            return

        try:
            random_id = secrets.randbits(31)
            params = {
                "peer_id": int(recipient_id),
                "sticker_id": int(sticker_id),
                "random_id": random_id,
                "group_id": self._config.group_id,
            }
            res = await self._vk_call("messages.send", params)
            logger.info("[vk] SENT sticker: peer_id=%s sticker_id=%s", recipient_id, sticker_id)
        except Exception as e:
            logger.exception("[vk] Failed to send sticker to %s: %s", recipient_id, e)

    async def send_contact(self, recipient_id: str, content: ContactContent) -> None:
        """Send a contact via VK messages.send."""
        # VK doesn't have native contact cards like WhatsApp/Telegram
        # Send as formatted text
        contact_text = f"Контакт:\n{content.name}"
        if content.org:
            contact_text += f"\nОрганизация: {content.org}"
        contact_text += f"\nТелефон: {content.phone}"

        text_content = TextContent(type="text", text=contact_text)
        await self.send_text(recipient_id, text_content)

    async def send_location(self, recipient_id: str, content: LocationContent) -> None:
        """Send a location via VK messages.send."""
        # VK requires geo parameter in messages.send
        try:
            random_id = secrets.randbits(31)
            params = {
                "peer_id": int(recipient_id),
                "random_id": random_id,
                "group_id": self._config.group_id,
                "lat": content.latitude,
                "long": content.longitude,
            }
            if content.name:
                params["message"] = content.name
            
            res = await self._vk_call("messages.send", params)
            logger.info(
                "[vk] SENT location: peer_id=%s lat=%s long=%s",
                recipient_id,
                content.latitude,
                content.longitude,
            )
        except Exception as e:
            logger.exception("[vk] Failed to send location to %s: %s", recipient_id, e)
