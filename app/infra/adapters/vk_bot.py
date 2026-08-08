import logging
import secrets
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import httpx  # NEW
from pyee.asyncio import AsyncIOEventEmitter

from app.config import VKCommunityConfig
from app.domain.message import MediaContent, TextContent, UnifiedMessage
from app.domain.ports import MessengerAdapter, OnMessage
from app.infra.vk_media_service import VKMediaService

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
        self._http: Optional[httpx.AsyncClient] = None  # NEW
        self._media_service: Optional[VKMediaService] = None

    def on_message(self, cb: OnMessage) -> None:
        self._cb = cb

    def confirmation_token(self) -> str:
        return self._config.confirmation

    async def start(self) -> None:
        # Initialize HTTP client once
        if self._http is None:
            self._http = httpx.AsyncClient(
                base_url="https://api.vk.ru/method",
                timeout=15,
                headers={"User-Agent": "chatwoot-integration/1.0"},
            )
        
        # Initialize media service
        if self._media_service is None:
            self._media_service = VKMediaService(
                access_token=self._config.access_token,
                group_id=self._config.group_id,
                api_version=self._config.api_version,
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
            
            # Process attachments if present
            attachments = msg.get("attachments", [])
            files_bytes = []
            filenames = []
            
            if attachments and self._media_service:
                try:
                    files_bytes, filenames = await self._media_service.process_incoming_attachments(attachments)
                except Exception as e:
                    logger.exception("[vk] Failed to process attachments: %s", e)

            if not self._cb or not peer_id:
                logger.debug("[vk] skip incoming: no callback or missing peer_id")
                return

            # Build content based on whether we have text or attachments
            if files_bytes:
                # Use first file for MediaContent (VK typically sends one attachment per message)
                # If multiple attachments, they come as separate messages
                content = MediaContent(
                    type="media",
                    media_type="image",  # default, will be refined
                    url=f"data:application/octet-stream;base64,",  # placeholder - actual file in raw
                    caption=text if text else None,
                    filename=filenames[0] if filenames else None,
                )
                # Store file bytes in raw for downstream processing
                raw_data = {**payload, "_files": files_bytes, "_filenames": filenames}
            else:
                content = TextContent(type="text", text=text)
                raw_data = payload

            umsg = UnifiedMessage(
                channel="vk",
                sender_id=from_id,
                recipient_id=peer_id,
                message_id=message_id,
                content=content,
                raw=raw_data,
            )
            await self._cb(umsg)

        async def _on_vk_confirmation(payload: Dict[str, Any]) -> None:
            group_id = payload.get("group_id")
            logger.info("[vk] confirmation request received for group_id=%s", group_id)

        self._incoming_listener = _on_vk_incoming
        self._confirm_listener = _on_vk_confirmation
        self._bus.on("vk.incoming", self._incoming_listener)
        self._bus.on("vk.confirmation", self._confirm_listener)

        logger.info("[vk] adapter started (callback API, text only)")

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
        
        # Close media service
        if self._media_service:
            try:
                await self._media_service.close()
            except Exception:
                pass
            self._media_service = None

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
        """
        Send media message to VK.
        
        This method handles outgoing messages from Chatwoot that contain attachments.
        It downloads files from Chatwoot URLs and uploads them to VK servers.
        """
        logger.info("[vk] send_media called: media_type=%s url=%s filename=%s caption=%s", 
                    content.media_type, content.url, content.filename, content.caption)
        
        # For outgoing messages from Chatwoot, the URL will be a downloadable link
        # We need to get the actual file bytes from Chatwoot's attachment URLs
        # The content.url should be a valid HTTP URL to the file
        
        if not self._media_service:
            logger.error("[vk] Media service not initialized")
            return
        
        try:
            # Download file from Chatwoot URL
            logger.info("[vk] Downloading file from Chatwoot: %s", content.url)
            file_bytes = await self._media_service.download_file(str(content.url), timeout=60.0)
            logger.info("[vk] Downloaded %d bytes from %s", len(file_bytes), content.url)
            
            # Determine filename and MIME type
            filename = content.filename or f"file_{secrets.randbits(32)}"
            mime_type = content.mime_type or "application/octet-stream"
            logger.info("[vk] File info: filename=%s mime_type=%s", filename, mime_type)
            
            # Determine upload method based on media type
            attachment_string = None
            peer_id = int(recipient_id)
            
            if content.media_type == "image":
                logger.info("[vk] Uploading photo...")
                attachment_string = await self._media_service.upload_and_save_photo(
                    file_bytes=file_bytes,
                    peer_id=peer_id
                )
                logger.info("[vk] Photo upload result: %s", attachment_string)
            elif content.media_type == "audio":
                # Check if it's a voice message (ogg format)
                if filename.endswith(".ogg") or mime_type == "audio/ogg":
                    logger.info("[vk] Uploading voice message (ogg)...")
                    attachment_string = await self._media_service.upload_and_save_audio_message(
                        file_bytes=file_bytes,
                        peer_id=peer_id,
                        filename=filename
                    )
                    logger.info("[vk] Voice message upload result: %s", attachment_string)
                else:
                    # Regular audio document
                    logger.info("[vk] Uploading audio as document...")
                    attachment_string = await self._media_service.upload_and_save_document(
                        file_bytes=file_bytes,
                        peer_id=peer_id,
                        filename=filename,
                        mime_type=mime_type
                    )
                    logger.info("[vk] Audio document upload result: %s", attachment_string)
            else:
                # Document (video, file, etc.)
                logger.info("[vk] Uploading document (type=%s)...", content.media_type)
                attachment_string = await self._media_service.upload_and_save_document(
                    file_bytes=file_bytes,
                    peer_id=peer_id,
                    filename=filename,
                    mime_type=mime_type
                )
                logger.info("[vk] Document upload result: %s", attachment_string)
            
            if attachment_string:
                # Send message with attachment
                random_id = secrets.randbits(31)
                params = {
                    "peer_id": peer_id,
                    "random_id": random_id,
                    "attachment": attachment_string,
                    "group_id": self._config.group_id,
                }
                
                # Add caption/text if present
                if content.caption:
                    params["message"] = content.caption
                    logger.info("[vk] Sending media with caption: %s", content.caption)
                else:
                    logger.info("[vk] Sending media without caption")
                
                res = await self._vk_call("messages.send", params)
                logger.info("[vk] SENT MEDIA: peer_id=%s attachment=%s result=%s", recipient_id, attachment_string, res)
            else:
                logger.error("[vk] Failed to upload media, no attachment string returned")
                
        except Exception as e:
            logger.exception("[vk] Failed to send media to %s: %s", recipient_id, e)
