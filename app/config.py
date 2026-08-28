import os
from typing import Dict, Optional

from pydantic import BaseModel, Field, HttpUrl, ValidationError


class TelegramConfig(BaseModel):
    api_id: int
    api_hash: str
    session_name: str
    inbox_id: int  # per-channel inbox


class WasenderWebhookConfig(BaseModel):
    webhook_id: str
    webhook_secret: str
    api_key: str
    inbox_id: int  # per-channel inbox

class MaxConfig(BaseModel):
    access_token: str
    bot_id: str  # ID бота (получается из GET /me)
    webhook_secret: str  # секрет для проверки webhook
    inbox_id: int
    webhook_id: str = "max"
    auto_subscribe: bool = True


class VKCommunityConfig(BaseModel):
    # VK community configuration for Callback API and sending messages
    callback_id: str  # Unique callback ID for path-based security
    group_id: int  # VK group ID (without minus)
    access_token: str  # VK community access token
    secret: str  # Secret key for callback signature verification
    confirmation: str  # Confirmation string from VK
    api_version: str = "5.199"  # VK API version
    inbox_id: int  # per-channel inbox


class ChatwootWebhookConfig(BaseModel):
    api_access_token: str
    account_id: int
    base_url: HttpUrl
    # Map webhook id -> channel name
    channel_by_webhook_id: Dict[str, str] = Field(default_factory=dict)
    # webhook_id -> secret
    secrets_by_webhook_id: Dict[str, str] = Field(default_factory=dict)

class OKConfig(BaseModel):
    access_token: str
    group_id: Optional[str] = None
    inbox_id: int
    webhook_id: str
    auto_subscribe: bool = True

class AppConfig(BaseModel):
    telegram: Optional[TelegramConfig] = None
    wasender: Optional[WasenderWebhookConfig] = None
    vk: Optional[VKCommunityConfig] = None
    chatwoot: ChatwootWebhookConfig
    ok: Optional[OKConfig] = None
    max: Optional[MaxConfig] = None
    gateway_base_url: Optional[str] = None

 


def _getenv(name: str) -> str:
    """Get required environment variable or raise RuntimeError."""
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


def _build_channel_map() -> Dict[str, str]:
    """Build a map from webhook ID to channel name."""
    mapping: Dict[str, str] = {}
    w = os.getenv("CHATWOOT_WEBHOOK_ID_WHATSAPP")
    t = os.getenv("CHATWOOT_WEBHOOK_ID_TELEGRAM")
    v = os.getenv("CHATWOOT_WEBHOOK_ID_VK")
    o = os.getenv("CHATWOOT_WEBHOOK_ID_OK")
    m = os.getenv("CHATWOOT_WEBHOOK_ID_MAX")
    if w:
        mapping[w] = "whatsapp"
    if t:
        mapping[t] = "telegram"
    if v:
        mapping[v] = "vk"
    if o:
        mapping[o] = "ok"
    if m:
        mapping[m] = "max"
    return mapping

def _build_secret_map() -> Dict[str, str]:
    """Build a map from webhook ID to secret."""
    mapping: Dict[str, str] = {}
    
    secrets = {
        "whatsapp": os.getenv("CHATWOOT_WEBHOOK_SECRET_WHATSAPP"),
        "telegram": os.getenv("CHATWOOT_WEBHOOK_SECRET_TELEGRAM"),
        "vk": os.getenv("CHATWOOT_WEBHOOK_SECRET_VK"),
        "ok": os.getenv("CHATWOOT_WEBHOOK_SECRET_OK"),
        "max": os.getenv("CHATWOOT_WEBHOOK_SECRET_MAX"),
    }
    
    webhook_ids = {
        "whatsapp": os.getenv("CHATWOOT_WEBHOOK_ID_WHATSAPP"),
        "telegram": os.getenv("CHATWOOT_WEBHOOK_ID_TELEGRAM"),
        "vk": os.getenv("CHATWOOT_WEBHOOK_ID_VK"),
        "ok": os.getenv("CHATWOOT_WEBHOOK_ID_OK"),
        "max": os.getenv("CHATWOOT_WEBHOOK_ID_MAX"),
    }
    
    for channel, webhook_id in webhook_ids.items():
        secret = secrets.get(channel)
        if webhook_id and secret:
            mapping[webhook_id] = secret
    
    return mapping

def load_config() -> AppConfig:
    try:
        # Telegram config: only if all variables are present
        if (
            os.getenv("TG_API_ID")
            and os.getenv("TG_API_HASH")
            and os.getenv("TG_SESSION_NAME")
        ):
            telegram_cfg = TelegramConfig(
                api_id=int(_getenv("TG_API_ID")),
                api_hash=_getenv("TG_API_HASH"),
                session_name=_getenv("TG_SESSION_NAME"),
                inbox_id=int(os.getenv("TG_INBOX_ID")),
            )
        else:
            telegram_cfg = None

        # Wasender config: only if all variables are present
        if (
            os.getenv("WASENDER_WEBHOOK_ID")
            and os.getenv("WASENDER_WEBHOOK_SECRET")
            and os.getenv("WASENDER_API_KEY")
        ):
            wasender_cfg = WasenderWebhookConfig(
                webhook_id=_getenv("WASENDER_WEBHOOK_ID"),
                webhook_secret=_getenv("WASENDER_WEBHOOK_SECRET"),
                api_key=_getenv("WASENDER_API_KEY"),
                inbox_id=int(os.getenv("WASENDER_INBOX_ID")),
            )
        else:
            wasender_cfg = None

        # VK: create config only if all required variables are present
        if (
            os.getenv("VK_CALLBACK_ID")
            and os.getenv("VK_GROUP_ID")
            and os.getenv("VK_ACCESS_TOKEN")
            and os.getenv("VK_SECRET")
            and os.getenv("VK_CONFIRMATION")
        ):
            vk_cfg = VKCommunityConfig(
                callback_id=_getenv("VK_CALLBACK_ID"),
                group_id=int(_getenv("VK_GROUP_ID")),
                access_token=_getenv("VK_ACCESS_TOKEN"),
                secret=_getenv("VK_SECRET"),
                confirmation=_getenv("VK_CONFIRMATION"),
                api_version=os.getenv("VK_API_VERSION") or "5.199",
                inbox_id=int(os.getenv("VK_INBOX_ID")),
            )
        else:
            vk_cfg = None

        ok_cfg = None
        if os.getenv("OK_ACCESS_TOKEN") and os.getenv("OK_INBOX_ID"):
            ok_cfg = OKConfig(
                access_token=_getenv("OK_ACCESS_TOKEN"),
                group_id=os.getenv("OK_GROUP_ID"),
                inbox_id=int(_getenv("OK_INBOX_ID")),
                webhook_id=os.getenv("OK_WEBHOOK_ID") or "228",
                auto_subscribe=os.getenv("OK_AUTO_SUBSCRIBE", "true").lower() == "true",
            )

        # MAX config
        max_cfg = None
        if os.getenv("MAX_ACCESS_TOKEN") and os.getenv("MAX_INBOX_ID"):
            max_cfg = MaxConfig(
                access_token=_getenv("MAX_ACCESS_TOKEN"),
                bot_id=os.getenv("MAX_BOT_ID", ""),  # может быть пустым, получим через /me
                webhook_secret=_getenv("MAX_WEBHOOK_SECRET"),
                inbox_id=int(_getenv("MAX_INBOX_ID")),
                webhook_id=os.getenv("MAX_WEBHOOK_ID") or "max",
                auto_subscribe=os.getenv("MAX_AUTO_SUBSCRIBE", "true").lower() == "true",
            )

        return AppConfig(
            telegram=telegram_cfg,
            wasender=wasender_cfg,
            vk=vk_cfg,
            ok=ok_cfg,
            max=max_cfg,
            gateway_base_url=os.getenv("GATEWAY_BASE_URL"),  # НОВОЕ
            chatwoot=ChatwootWebhookConfig(
                api_access_token=_getenv("CHATWOOT_API_ACCESS_TOKEN"),
                account_id=int(_getenv("CHATWOOT_ACCOUNT_ID")),
                base_url=_getenv("CHATWOOT_BASE_URL"),
                channel_by_webhook_id=_build_channel_map(),
                secrets_by_webhook_id=_build_secret_map(),
            ),
        )


    
    except ValidationError as e:
        raise RuntimeError(f"Invalid configuration: {e}") from e
