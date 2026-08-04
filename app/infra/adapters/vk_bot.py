import io
import logging
import secrets
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import httpx  # NEW
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

    def capabilities(self) -> set:
        return {"text", "media"}

    async def start(self) -> None:
        # Initialize HTTP client once
        if self._http is None:
            self._http = httpx.AsyncClient(
                base_url="https://api.vk.ru/method",
                timeout=30,
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
            
            # Parse attachments from VK
            attachments = msg.get("attachments") or []
            content = await self._parse_vk_attachments(attachments, text)
            
            if not self._cb or not peer_id:
                logger.debug("[vk] skip incoming: no callback or missing peer_id")
                return

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

    async def _parse_vk_attachments(
        self, attachments: List[Dict[str, Any]], text: str = ""
    ) -> TextContent | MediaContent:
        """
        Parse VK attachments array into UnifiedMessage content.
        Returns MediaContent for the first media attachment, or TextContent if no media.
        Supported types: photo, audio_message (voice), doc, video, audio.
        """
        if not attachments:
            return TextContent(type="text", text=text)

        # Process first attachment for now (can be extended for multiple)
        att = attachments[0]
        att_type = att.get("type")
        
        # Map VK attachment types to MediaContent
        media_type_map = {
            "photo": "image",
            "video": "video",
            "audio_message": "audio",
            "doc": "document",
            "audio": "audio",
        }
        
        if att_type in media_type_map:
            media_type = media_type_map[att_type]
            obj = att.get(att_type, {})
            
            # Get URL based on type
            url = ""
            filename = None
            mime_type = None
            
            if att_type == "photo":
                # Photos have multiple sizes, get the largest available
                url = (
                    obj.get("photo_2560")
                    or obj.get("photo_1280")
                    or obj.get("photo_807")
                    or obj.get("photo_604")
                    or obj.get("photo_340")
                    or obj.get("photo_200")
                    or obj.get("photo_130")
                    or ""
                )
                mime_type = "image/jpeg"
            elif att_type == "audio_message":
                url = obj.get("link", "")
                filename = obj.get("title", "voice_message.ogg")
                mime_type = "audio/ogg"
            elif att_type == "doc":
                url = obj.get("url", "")
                filename = obj.get("title", "document")
                mime_type = obj.get("mime_type", "application/octet-stream")
            elif att_type == "video":
                url = obj.get("player", "") or obj.get("photo_800", "")
                mime_type = "video/mp4"
            elif att_type == "audio":
                url = obj.get("url", "")
                filename = f"{obj.get('artist', '')} - {obj.get('title', 'audio')}"
                mime_type = "audio/mpeg"
            
            caption = text if text else None
            
            return MediaContent(
                type="media",
                media_type=media_type,  # type: ignore
                url=url,
                caption=caption,
                filename=filename,
                mime_type=mime_type,
            )
        
        # If no recognized media type, fall back to text
        return TextContent(type="text", text=text)

    async def send_text(self, recipient_id: str, content: TextContent) -> None:
        """Send a text message via VK messages.send."""
        text = content.text or ""
        if not text:
            logger.info("[vk] skip send: empty text")
            return

        try:
            random_id = secrets.randbits(31)
            params = {
                "peer_id": int(recipient_id),
                "message": text,
                "random_id": random_id,
                "group_id": self._config.group_id,
            }
            res = await self._vk_call("messages.send", params)
            logger.info("[vk] SENT: peer_id=%s message_id=%s", recipient_id, res)
        except Exception as e:
            logger.exception("[vk] Failed to send text to %s: %s", recipient_id, e)

    async def send_media(self, recipient_id: str, content: MediaContent) -> None:
        """
        Send media (photo, document, audio_message) via VK.
        
        Process:
        1. Download media from URL
        2. Get upload server from VK API
        3. Upload file to server
        4. Save uploaded media
        5. Send message with attachment
        """
        url = str(content.url)
        media_type = content.media_type
        caption = content.caption or ""
        filename = content.filename
        mime_type = content.mime_type
        
        try:
            # Step 1: Download media from URL
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                file_bytes = resp.content
            
            # Determine VK attachment type and API methods
            if media_type == "image":
                upload_method = "photos.getMessagesUploadServer"
                save_method = "photos.saveMessagesPhoto"
                att_prefix = "photo"
            elif media_type in ("audio", "audio_message"):
                # Voice messages use docs API
                upload_method = "docs.getMessagesUploadServer"
                save_method = "docs.save"
                att_prefix = "doc"
                if not filename:
                    filename = "voice_message.ogg"
            else:  # document, video (video as doc for now)
                upload_method = "docs.getMessagesUploadServer"
                save_method = "docs.save"
                att_prefix = "doc"
                if not filename:
                    filename = "document"
            
            # Step 2: Get upload server
            upload_params = {"group_id": self._config.group_id}
            if upload_method.startswith("docs"):
                upload_params["type"] = "audio_message" if media_type in ("audio", "audio_message") else "doc"
            
            upload_resp = await self._vk_call(upload_method, upload_params)
            upload_url = upload_resp.get("upload_url")
            if not upload_url:
                raise RuntimeError(f"No upload URL from {upload_method}")
            
            # Step 3: Upload file to server
            files = {"file": (filename, io.BytesIO(file_bytes), mime_type or "application/octet-stream")}
            async with httpx.AsyncClient(timeout=60) as upload_client:
                upload_response = await upload_client.post(upload_url, files=files)
                upload_response.raise_for_status()
                upload_data = upload_response.json()
            
            # Step 4: Save uploaded media
            if upload_method.startswith("photos"):
                save_params = {
                    "photo": upload_data.get("photo", ""),
                    "server": upload_data.get("server", 0),
                    "hash": upload_data.get("hash", ""),
                }
            else:  # docs
                save_params = {
                    "file": upload_data.get("file", ""),
                }
            
            saved = await self._vk_call(save_method, save_params)
            
            # Extract attachment ID
            if isinstance(saved, list) and len(saved) > 0:
                saved = saved[0]
            
            owner_id = saved.get("owner_id", -self._config.group_id)
            media_id = saved.get("id")
            if not media_id:
                raise RuntimeError(f"No media ID in save response: {saved}")
            
            # Form attachment string: {type}{owner_id}_{media_id}
            attachment = f"{att_prefix}{owner_id}_{media_id}"
            
            # Step 5: Send message with attachment
            random_id = secrets.randbits(31)
            send_params = {
                "peer_id": int(recipient_id),
                "attachment": attachment,
                "random_id": random_id,
                "group_id": self._config.group_id,
            }
            if caption:
                send_params["message"] = caption
            
            res = await self._vk_call("messages.send", send_params)
            logger.info(
                "[vk] SENT MEDIA: peer_id=%s type=%s attachment=%s result=%s",
                recipient_id,
                media_type,
                attachment,
                res,
            )
        except Exception as e:
            logger.exception("[vk] Failed to send media to %s: %s", recipient_id, e)

    async def send_sticker(
        self, recipient_id: str, content: StickerContent
    ) -> None:
        """Send sticker - not yet implemented for VK."""
        logger.warning("[vk] send_sticker not implemented")

    async def send_contact(
        self, recipient_id: str, content: ContactContent
    ) -> None:
        """Send contact - not yet implemented for VK."""
        logger.warning("[vk] send_contact not implemented")

    async def send_location(
        self, recipient_id: str, content: LocationContent
    ) -> None:
        """Send location - not yet implemented for VK."""
        logger.warning("[vk] send_location not implemented")
