import json
import logging
from typing import Any, Dict

from fastapi import APIRouter, Header, HTTPException, Request
from pyee.asyncio import AsyncIOEventEmitter
from starlette.responses import PlainTextResponse

from app.config import AppConfig
from app.domain.webhooks.wasender import WasenderWebhookPayload

import hmac
import hashlib
import time
from datetime import datetime

logger = logging.getLogger(__name__)

MAX_TIMESTAMP_AGE_SECONDS = 300  # 5 минут

def verify_chatwoot_signature(
    *,
    secret: str,
    raw_body: bytes,
    signature_header: str | None,
    timestamp_header: str | None,
) -> bool:
    """
    Проверяет HMAC-SHA256 подпись webhook от Chatwoot.
    
    Формула: sha256=HMAC-SHA256(secret, "{timestamp}.{raw_body}")
    
    Args:
        secret: секрет webhook из Chatwoot
        raw_body: сырое тело запроса (bytes, не парсится!)
        signature_header: значение заголовка X-Chatwoot-Signature
        timestamp_header: значение заголовка X-Chatwoot-Timestamp
    
    Returns:
        True если подпись валидна, False иначе
    """
    if not signature_header or not timestamp_header:
        logger.warning("[chatwoot] Missing signature headers")
        return False
    
    # Проверка timestamp (защита от replay-атак)
    try:
        timestamp = int(timestamp_header)
    except (ValueError, TypeError):
        logger.warning("[chatwoot] Invalid timestamp: %s", timestamp_header)
        return False
    
    now = int(time.time())
    age = abs(now - timestamp)
    if age > MAX_TIMESTAMP_AGE_SECONDS:
        logger.warning(
            "[chatwoot] Timestamp too old: %s (age=%ds, max=%ds)",
            datetime.fromtimestamp(timestamp).isoformat(),
            age,
            MAX_TIMESTAMP_AGE_SECONDS,
        )
        return False
    
    # Проверяем префикс sha256=
    if not signature_header.startswith("sha256="):
        logger.warning("[chatwoot] Invalid signature prefix: %s", signature_header[:20])
        return False
    
    received_signature = signature_header[len("sha256="):]
    
    # Вычисляем ожидаемую подпись
    # Важно: используем raw_body как bytes, не парсим JSON!
    signed_payload = f"{timestamp}.".encode("utf-8") + raw_body
    expected_signature = hmac.new(
        secret.encode("utf-8"),
        signed_payload,
        hashlib.sha256,
    ).hexdigest()
    
    # Constant-time comparison (защита от timing attacks)
    is_valid = hmac.compare_digest(expected_signature, received_signature)
    
    if not is_valid:
        logger.warning(
            "[chatwoot] Signature mismatch: expected=%s, received=%s",
            expected_signature[:16] + "...",
            received_signature[:16] + "...",
        )
    
    return is_valid

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
    async def chatwoot_webhook(
        webhook_id: str,
        request: Request,
        x_chatwoot_signature: str | None = Header(default=None, alias="X-Chatwoot-Signature"),
        x_chatwoot_timestamp: str | None = Header(default=None, alias="X-Chatwoot-Timestamp"),
    ):
        channel = config.chatwoot.channel_by_webhook_id.get(webhook_id)
        if not channel:
            raise HTTPException(status_code=403, detail=f"Unknown webhook ID: {webhook_id}")
        
        # === ВАЖНО: читаем сырое тело ДО парсинга JSON ===
        raw_body = await request.body()
        
        # === Проверка подписи ===
        secret = config.chatwoot.secrets_by_webhook_id.get(webhook_id)
        if secret:
            if not verify_chatwoot_signature(
                secret=secret,
                raw_body=raw_body,
                signature_header=x_chatwoot_signature,
                timestamp_header=x_chatwoot_timestamp,
            ):
                raise HTTPException(status_code=401, detail="Invalid webhook signature")
        else:
            # Секрет не настроен — логируем предупреждение, но пропускаем
            logger.warning(
                "[chatwoot] No secret configured for webhook_id=%s, skipping signature check",
                webhook_id,
            )
        
        # Парсим JSON из raw body
        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")
        
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
            # Обрабатываем только outgoing
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

    @router.post("/max/webhook/{webhook_id}", response_model=dict)
    async def max_webhook(webhook_id: str, request: Request):
        """Webhook для MAX сообщений."""
        if not getattr(config, "max", None):
            raise HTTPException(status_code=503, detail="MAX adapter not configured")

        if webhook_id != config.max.webhook_id:
            raise HTTPException(status_code=403, detail="Invalid webhook ID")

        try:
            payload: Dict[str, Any] = await request.json()
            logger.info("[max] webhook payload: %s", json.dumps(payload, ensure_ascii=False, indent=2))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON")

        # Проверка подписи (если секрет настроен)
        # MAX отправляет заголовок X-Max-Signature или проверяет через body
        # Пока оставляем без проверки (можно добавить позже)

        update_type = payload.get("update_type")
        logger.info("[max] webhook received: update_type=%s", update_type)

        if update_type == "message_created":
            bus.emit("max.incoming", payload)
        else:
            logger.info("[max] ignored update_type: %s", update_type)

        return {"status": "ok"}

    return router


