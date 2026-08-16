import logging
from typing import Any, Dict

from fastapi import APIRouter, Header, HTTPException, Request
from pyee.asyncio import AsyncIOEventEmitter
from starlette.responses import PlainTextResponse

from app.config import AppConfig
from app.domain.webhooks.wasender import WasenderWebhookPayload

logger = logging.getLogger(__name__)


def create_router(bus: AsyncIOEventEmitter, config: AppConfig) -> APIRouter:
    """
    Build HTTP routes with simple security checks.
    """
    router = APIRouter(tags=["webhooks"])

    @router.get("/health")
    async def health():
        # Report only non-sensitive fields
        wasender_enabled = bool(getattr(config, "wasender", None))
        telegram_enabled = bool(getattr(config, "telegram", None))
        vk_enabled = bool(getattr(config, "vk", None))

        return {
            "ok": True,
            "chatwoot": {
                "account_id": config.chatwoot.account_id,
                "inbox_id": config.chatwoot.inbox_id,
                "base_url": str(config.chatwoot.base_url),
                "channels_configured": list(
                    config.chatwoot.channel_by_webhook_id.values()
                ),
            },
            "wasender": {
                "enabled": wasender_enabled,
            },
            "telegram": {
                "enabled": telegram_enabled,
                "session_name": (
                    config.telegram.session_name if telegram_enabled else None
                ),
            },
            "vk": {
                "enabled": vk_enabled,
                # Do not expose callback_id/secret/token; group_id is safe to show
                "group_id": config.vk.group_id if vk_enabled else None,
            },
        }

    @router.post("/wasender/webhook/{webhook_id}", response_model=dict)
    async def wasender_webhook(
        webhook_id: str,
        payload: WasenderWebhookPayload,
        x_webhook_signature: str | None = Header(
            default=None, alias="X-Webhook-Signature"
        ),
    ):
        # Verify path token first
        if webhook_id != config.wasender.webhook_id:
            raise HTTPException(status_code=403, detail="Invalid webhook ID")
        # Simple header equality check (no HMAC)
        if x_webhook_signature != config.wasender.webhook_secret:
            raise HTTPException(status_code=403, detail="Invalid X-Webhook-Signature")

        event = payload.event
        logger.info("[http] Wasender webhook accepted: event=%s", event)

        if event == "messages.upsert":
            try:
                raw = payload.data["messages"]
                key = raw["key"]
                from_me = key["fromMe"]
                bus.emit(
                    "wasender.outgoing" if from_me else "wasender.incoming",
                    payload.model_dump(),
                )
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Invalid upsert format: {e}"
                )
        else:
            logger.info("[wasender] Ignored event: %s", event)

        return {"status": "ok"}

    @router.post("/chatwoot/webhook/{webhook_id}", response_model=dict)
    async def chatwoot_webhook(webhook_id: str, request: Request):
        channel = config.chatwoot.channel_by_webhook_id.get(webhook_id)
        if not channel:
            raise HTTPException(status_code=403, detail=f"Unknown webhook ID: {webhook_id}")
    
        payload = await request.json()
        event = payload.get("event")
        msg_type = payload.get("message_type")
    
        # Инжектим channel в метаданные
        conv = payload.setdefault("conversation", {})
        meta = conv.setdefault("meta", {})
        meta["channel"] = channel
    
        logger.info(
            "[http] Chatwoot webhook accepted: event=%s type=%s channel=%s",
            event, msg_type, channel,
        )
    
        if event == "message_created":
            # === ВАЖНО: обрабатываем только outgoing ===
            # Входящие (которые мы же создали через API) не обрабатываем повторно
            if msg_type == "outgoing":
                bus.emit("chatwoot.outgoing", payload)
            else:
                logger.debug("[chatwoot] Ignored incoming webhook (created via API)")
        else:
            logger.info("[chatwoot] Ignored event: %s", event)
    
        return {"status": "received"}

    @router.post("/vk/callback/{callback_id}", response_class=PlainTextResponse)
    async def vk_callback(callback_id: str, request: Request) -> PlainTextResponse:
        """
        VK Callback endpoint with path-based security and confirmation support.

        - Verifies path callback_id first.
        - On 'confirmation' returns confirmation token (no secret required).
        - On other events verifies 'secret' and 'group_id'.
        - Emits 'vk.incoming' on 'message_new' and 'vk.confirmation' on confirmation.
        - Responds with plain text as VK requires.
        """
        if not getattr(config, "vk", None):
            raise HTTPException(status_code=503, detail="VK adapter is not configured")

        # Verify callback_id from path
        if callback_id != config.vk.callback_id:
            raise HTTPException(status_code=403, detail="Invalid callback ID")

        try:
            payload: Dict[str, Any] = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON body")

        event_type = payload.get("type")
        group_id = payload.get("group_id")
        secret = payload.get("secret")

        logger.info("[vk] event received: type=%s group_id=%s", event_type, group_id)

        # Handle confirmation (no secret required)
        if event_type == "confirmation":
            if group_id != config.vk.group_id:
                raise HTTPException(status_code=400, detail="Invalid group_id")
            # Optional: emit confirmation event for debugging/metrics
            bus.emit("vk.confirmation", {"group_id": group_id})
            return PlainTextResponse(config.vk.confirmation)

        # For all other events, verify secret and group_id
        if secret != config.vk.secret:
            raise HTTPException(status_code=403, detail="Invalid secret")
        if group_id != config.vk.group_id:
            raise HTTPException(status_code=400, detail="Invalid group_id")

        if event_type == "message_new":
            try:
                obj = payload.get("object") or {}
                message = obj.get("message") or {}
                # Emit unified internal event; VkAdapter will convert to UnifiedMessage
                bus.emit(
                    "vk.incoming",
                    {"event": "message_new", "message": message, "raw": payload},
                )
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Invalid message_new payload: {e}"
                )
        else:
            # Acknowledge other events to prevent VK retries
            logger.info("[vk] ignored event type: %s", event_type)

        # VK requires literal 'ok' to acknowledge processing
        return PlainTextResponse("ok")

    @router.post("/ok/callback/{webhook_id}", response_model=dict)
    async def ok_webhook(webhook_id: str, request: Request):
        """Webhook для получения сообщений из Одноклассников."""
        if not getattr(config, "ok", None):
            raise HTTPException(status_code=503, detail="OK adapter is not configured")

        # Проверяем webhook_id (защита от случайных запросов)
        if webhook_id != config.ok.webhook_id:
            raise HTTPException(status_code=403, detail="Invalid webhook ID")

        try:
            payload: Dict[str, Any] = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON body")

        webhook_type = payload.get("webhookType")
        logger.info("[ok] event received: type=%s", webhook_type)

        # Обрабатываем разные типы событий
        if webhook_type == "MESSAGE_CREATED":
            bus.emit("ok.incoming", payload)
        elif webhook_type == "CHAT_SYSTEM":
            logger.info("[ok] system event: %s", payload.get("type"))
        else:
            logger.info("[ok] ignored webhook type: %s", webhook_type)

        # ОК требует ответ 200 OK в течение 5 секунд
        return {"status": "ok"}

    return router
