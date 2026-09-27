import pytest
from httpx import AsyncClient, ASGITransport
from pyee.asyncio import AsyncIOEventEmitter
from app.delivery.http import create_router
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional


@pytest.fixture
def mock_config():
    class MockConfig:
        class MockVKConfig:
            callback_id = "test_callback"
            secret = "test_secret"
            group_id = 123456
            confirmation = "test_confirmation"

        vk = MockVKConfig()

        class MockChatwootConfig:
            channel_by_webhook_id = {}
            secrets_by_webhook_id = {}

        chatwoot = MockChatwootConfig()

        class MockWasenderConfig:
            webhook_id = "wa"
            webhook_secret = "secret"

        wasender = MockWasenderConfig()

        max = None
        ok = None
        telegram = None

    return MockConfig()


@pytest.fixture
def test_app(mock_config):
    app = FastAPI()
    bus = AsyncIOEventEmitter()

    # Capture events
    bus.emitted_events = []

    def emit_wrapper(event_name, payload):
        bus.emitted_events.append((event_name, payload))

    bus.emit = emit_wrapper

    router = create_router(bus=bus, config=mock_config)  # type: ignore
    app.include_router(router)
    app.bus = bus
    return app


@pytest.mark.asyncio
async def test_vk_callback_private_message(test_app):
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        payload = {
            "type": "message_new",
            "group_id": 123456,
            "secret": "test_secret",
            "object": {"message": {"peer_id": 123, "from_id": 123}},
        }
        response = await client.post("/vk/callback/test_callback", json=payload)
        assert response.status_code == 200
        assert response.text == "ok"

        # Verify event was emitted
        assert len(test_app.bus.emitted_events) == 1
        assert test_app.bus.emitted_events[0][0] == "vk.incoming"


@pytest.mark.asyncio
async def test_vk_callback_group_message(test_app):
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        payload = {
            "type": "message_new",
            "group_id": 123456,
            "secret": "test_secret",
            "object": {"message": {"peer_id": 2000000001, "from_id": 123}},
        }
        response = await client.post("/vk/callback/test_callback", json=payload)
        assert response.status_code == 200
        assert response.text == "ok"

        # Verify event was NOT emitted
        assert len(test_app.bus.emitted_events) == 0


@pytest.mark.asyncio
async def test_vk_callback_outgoing_message(test_app):
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        payload = {
            "type": "message_reply",
            "group_id": 123456,
            "secret": "test_secret",
            "object": {"peer_id": 123, "from_id": 456, "out": 1},
        }
        response = await client.post("/vk/callback/test_callback", json=payload)
        assert response.status_code == 200
        assert response.text == "ok"

        # Verify event WAS emitted (because it's an outgoing message)
        assert len(test_app.bus.emitted_events) == 1
        assert test_app.bus.emitted_events[0][0] == "vk.incoming"
