import logging
import secrets
from typing import Any, Awaitable, Callable, Dict, Optional

import httpx  # NEW
from pyee.asyncio import AsyncIOEventEmitter

from app.config import VKCommunityConfig
from app.domain.message import TextContent, UnifiedMessage
from app.domain.ports import MessengerAdapter, OnMessage

logger = logging.getLogger(__name__)


class VkAdapter(MessengerAdapter):
    """VK adapter for Callback API (text only)."""

    def __init__(self, bus: AsyncIOEventEmitter, config: VKCommunityConfig):
        self._bus = bus
        self._config = config
        self.inbox_id = config.inbox_id
        self._cb: Optional[OnMessage] = None
        self._incoming_listener: Optional[Callable[..., Awaitable[None]]] = None
        self._confirm_listener: Optional[Callable[..., Awaitable[None]]] = None
        self._http: Optional[httpx.AsyncClient] = None  # NEW

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

            content = TextContent(type="text", text=text)
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
