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

    router = create_router(bus=bus, config=mock_config) # type: ignore
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
            "object": {
                "message": {
                    "peer_id": 123,
                    "from_id": 123
                }
            }
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
            "object": {
                "message": {
                    "peer_id": 2000000001,
                    "from_id": 123
                }
            }
        }
        response = await client.post("/vk/callback/test_callback", json=payload)
        assert response.status_code == 200
        assert response.text == "ok"

        # Verify event was NOT emitted
        assert len(test_app.bus.emitted_events) == 0

@pytest.mark.asyncio
async def test_vk_callback_group_message_peer_neq_from(test_app):
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        payload = {
            "type": "message_new",
            "group_id": 123456,
            "secret": "test_secret",
            "object": {
                "message": {
                    "peer_id": 124,
                    "from_id": 123
                }
            }
        }
        response = await client.post("/vk/callback/test_callback", json=payload)
        assert response.status_code == 200
        assert response.text == "ok"

        # Verify event was NOT emitted
        assert len(test_app.bus.emitted_events) == 0

@pytest.mark.asyncio
async def test_vk_callback_outgoing_message_reply(test_app):
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        payload = {
            "group_id": 123456,
            "type": "message_reply",
            "event_id": "f018b386b54d1a300c018d5b9308c308a1eb3a34",
            "v": "5.199",
            "object": {
                "date": 1790537791,
                "from_id": -232408395,
                "id": 409,
                "version": 10000864,
                "out": 1,
                "fwd_messages": [],
                "important": False,
                "is_hidden": False,
                "attachments": [],
                "conversation_message_id": 319,
                "text": "Тестовое сообщение из шлюза 2",
                "peer_id": 1056538121,
                "random_id": -1939640752
            },
            "secret": "test_secret"
        }
        response = await client.post("/vk/callback/test_callback", json=payload)
        assert response.status_code == 200
        assert response.text == "ok"

        # Verify event was NOT emitted because it's outgoing
        assert len(test_app.bus.emitted_events) == 0
