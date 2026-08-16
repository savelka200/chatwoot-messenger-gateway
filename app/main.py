import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from pyee.asyncio import AsyncIOEventEmitter

from app.application.events import wire_events
from app.application.router import MessageRouter
from app.config import load_config
from app.delivery.http import create_router
from app.infra.adapters.telegram_telethon import TelegramAdapter
from app.infra.adapters.vk_bot import VkAdapter
from app.infra.adapters.whatsapp_wasender import WasenderAdapter
from app.infra.adapters.ok_bot import OKAdapter

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)

# Load env and config
load_dotenv()
config = load_config()

# Shared event bus
bus = AsyncIOEventEmitter()

# Build adapters registry only for configured channels
adapters: Dict[str, Any] = {}

if config.wasender:
    adapters["whatsapp"] = WasenderAdapter(bus=bus, config=config.wasender)

if config.telegram:
    adapters["telegram"] = TelegramAdapter(bus=bus, config=config.telegram)

if config.vk:
    adapters["vk"] = VkAdapter(bus=bus, config=config.vk)

if config.ok:
    ok_adapter = OKAdapter(bus=bus, config=config.ok)
    adapters["ok"] = ok_adapter


router = MessageRouter(adapters=adapters)

# Wire adapter incoming → application router (existing behavior)
for a in adapters.values():
    a.on_message(router.handle_incoming)

# Wire bus event handlers (moved out of main into application layer)
wire_events(bus=bus, config=config, adapters=adapters, router=router)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # === НОВОЕ: Автоматическая подписка на webhook ОК ===
    if config.ok and config.ok.auto_subscribe:
        ok_adapter = adapters.get("ok")
        if ok_adapter:
            webhook_url = f"{config.gateway_base_url.rstrip('/')}/ok/callback/{config.ok.webhook_id}"
            logging.info("[main] subscribing OK webhook to: %s", webhook_url)
            await ok_adapter.start()  # Запускаем адаптер перед подпиской
            success = await ok_adapter.subscribe_to_webhook(webhook_url)
            if not success:
                logging.warning("[main] OK webhook subscription failed, check manually")

    # Log here (server process only; avoids duplicate logs from reloader)
    logging.info("adapters configured: %s", list(adapters.keys()))
    await asyncio.gather(
        *(a.start() for a in adapters.values()), return_exceptions=True
    )
    try:
        yield
    finally:
        await asyncio.gather(
            *(a.stop() for a in adapters.values()), return_exceptions=True
        )

app = FastAPI(title="Messaging Bridge", version="0.1.0", lifespan=lifespan)
app.include_router(create_router(bus=bus, config=config))

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, log_level="info")
