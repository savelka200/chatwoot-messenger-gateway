import logging
from typing import Any, Dict, List, Mapping, Optional

import httpx
from pyee.asyncio import AsyncIOEventEmitter

from app.application.chatwoot_service import ChatwootService
from app.application.router import MessageRouter
from app.config import AppConfig
from app.infra.chatwoot_client import ChatwootClient
from app.infra.vk_media_service import VKMediaService

logger = logging.getLogger(__name__)


async def _fetch_vk_profile(
    access_token: str, api_version: str, user_id: str
) -> Dict[str, Any]:
    """
    Fetch minimal VK profile data needed for enrichment:
    - first_name, last_name (for contact.name)
    - bdate (for custom attribute vk_bdate)
    """
    url = "https://api.vk.ru/method/users.get"
    params = {
        "user_ids": user_id,
        "fields": "bdate,city,screen_name,photo_200",
        "access_token": access_token,
        "v": api_version,
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            resp = (data or {}).get("response") or []
            return resp[0] if resp else {}
    except Exception as e:
        logger.warning("[vk] users.get failed: %s", e)
        return {}


async def _process_vk_attachments_and_send_to_chatwoot(
    cw: ChatwootService,
    inbox_id: int,
    peer_id: str,
    from_id: str,
    text: str,
    files_bytes: List[bytes],
    filenames: List[str],
    vk_name: Optional[str],
    custom_attributes: Dict[str, Any],
    additional_attributes: Dict[str, Any],
    avatar_url: Optional[str],
) -> None:
    """
    Process VK incoming message with attachments and send to Chatwoot.
    Files are sent as multipart attachments to Chatwoot.
    """
    try:
        logger.info("[events-vk] Starting attachment processing: %d files", len(files_bytes))
        
        # Ensure contact first
        ensured = await cw.ensure_contact(
            inbox_id=inbox_id,
            search_key=from_id,
            name=vk_name or from_id,
            phone=None,
            email=None,
            custom_attributes=custom_attributes,
            additional_attributes=additional_attributes,
            avatar_url=avatar_url,
        )
        logger.info("[events-vk] Contact ensured: id=%s", ensured.get("id"))

        conv_id = await cw.ensure_conversation(
            inbox_id=inbox_id,
            contact_id=ensured["id"],
            source_id=ensured["source_id"],
        )
        logger.info("[events-vk] Conversation ensured: id=%s", conv_id)

        # Build files list for Chatwoot: (filename, file_bytes, mime_type)
        files = []
        for i, (file_bytes, filename) in enumerate(zip(files_bytes, filenames)):
            # Determine MIME type from filename
            mime_type = "application/octet-stream"
            if filename.endswith(".jpg") or filename.endswith(".jpeg"):
                mime_type = "image/jpeg"
            elif filename.endswith(".png"):
                mime_type = "image/png"
            elif filename.endswith(".gif"):
                mime_type = "image/gif"
            elif filename.endswith(".ogg"):
                mime_type = "audio/ogg"
            elif filename.endswith(".mp3"):
                mime_type = "audio/mpeg"
            elif filename.endswith(".mp4"):
                mime_type = "video/mp4"
            elif filename.endswith(".pdf"):
                mime_type = "application/pdf"
            elif filename.endswith(".doc") or filename.endswith(".docx"):
                mime_type = "application/msword"
            
            logger.info("[events-vk] File %d: %s (%d bytes, %s)", i, filename, len(file_bytes), mime_type)
            files.append((filename, file_bytes, mime_type))

        # Send message with attachments to Chatwoot
        if files:
            logger.info("[events-vk] Sending %d attachments to Chatwoot conv_id=%s", len(files), conv_id)
            for i, (fname, fbytes, fmime) in enumerate(files):
                logger.info("[events-vk] File %d details: name=%s size=%d mime=%s first_bytes=%r", 
                           i, fname, len(fbytes), fmime, fbytes[:50])
            
            # Check if content is empty but we have files - Chatwoot requires content or attachments
            content_to_send = text if text else None
            if not content_to_send and files:
                logger.info("[events-vk] No text content, sending only attachments")
            
            try:
                result = await cw._client.send_message_with_attachments(
                    conversation_id=conv_id,
                    content=content_to_send,
                    files=files,
                    message_type="incoming",
                )
                logger.info("[events-vk] Chatwoot API response: %s", result)
                
                # Log the response attachments to verify they were received
                if isinstance(result, dict):
                    attachments_in_response = result.get("attachments", [])
                    logger.info("[events-vk] Attachments in response: %d", len(attachments_in_response))
                    for att in attachments_in_response:
                        logger.info("[events-vk] Attachment: id=%s url=%s file_type=%s", 
                                   att.get("id"), att.get("url"), att.get("file_type"))
                
                logger.info(
                    "[events] vk -> chatwoot OK conv_id=%s inbox=%s with %d attachments",
                    conv_id, inbox_id, len(files)
                )
            except Exception as e:
                logger.exception("[events-vk] send_message_with_attachments failed: %s", e)
                raise
        else:
            # Fallback to text-only if no files processed
            logger.warning("[events-vk] No files to send, falling back to text-only")
            await cw.create_message(
                conversation_id=conv_id,
                content=text,
                direction="incoming",
            )
            logger.info(
                "[events] vk -> chatwoot OK conv_id=%s inbox=%s (text only fallback)",
                conv_id, inbox_id
            )
    except Exception as e:
        logger.exception("[events] vk attachments handling failed: %s", e)


def wire_events(
    bus: AsyncIOEventEmitter,
    config: AppConfig,
    adapters: Mapping[str, Any],
    router: MessageRouter,
) -> None:
    """
    Register application-level bus handlers.
    Incoming infra events are normalized and forwarded to ChatwootService.
    """
    cw_client = ChatwootClient(
        api_access_token=config.chatwoot.api_access_token,
        account_id=config.chatwoot.account_id,
        base_url=str(config.chatwoot.base_url),
    )
    cw = ChatwootService(client=cw_client)

    def _inbox_from_adapter(key: str) -> Optional[int]:
        a = adapters.get(key)
        return getattr(a, "inbox_id", None)

    @bus.on("wasender.incoming")
    async def _ingest_wa(payload: Dict[str, Any]) -> None:
        try:
            raw = payload["data"]["messages"]
            key = raw.get("key", {}) or {}
            msg = raw.get("message", {}) or {}

            text = (
                msg.get("conversation")
                or (msg.get("extendedTextMessage") or {}).get("text")
                or ""
            )
            remote = key.get("remoteJid") or key.get("participant") or ""
            msisdn = remote.split("@")[0] if "@" in remote else remote
            push_name = raw.get("pushName") or msisdn

            inbox_id = _inbox_from_adapter("whatsapp")
            if not inbox_id:
                raise RuntimeError("WhatsApp inbox_id is not configured")

            contact = await cw.ensure_contact(
                inbox_id=inbox_id,
                search_key=msisdn,
                name=push_name,
                phone=msisdn,
                email=None,
                custom_attributes={"wa_remote_jid": remote},
            )
            conv_id = await cw.ensure_conversation(
                inbox_id=inbox_id,
                contact_id=contact["id"],
                source_id=msisdn,
            )
            await cw.create_message(
                conversation_id=conv_id,
                content=(text or "").strip(),
                direction="incoming",
            )
            logger.info(
                "[events] wa -> chatwoot OK conv_id=%s inbox=%s", conv_id, inbox_id
            )
        except Exception as e:
            logger.exception("[events] wasender handling failed: %s", e)

    @bus.on("vk.incoming")
    async def _ingest_vk(payload: Dict[str, Any]) -> None:
        """
        VK (Callback API) incoming:
        - enrich contact with name (first+last; fallback to screen_name) and bdate
        - custom_attributes: vk_user_id, vk_peer_id, vk_bdate (if present)
        - additional_attributes: city (if present in users.get)
        - handle attachments if present
        """
        try:
            message = payload.get("message") or {}
            text = (message.get("text") or "").strip()
            peer_id = str(message.get("peer_id") or "")
            from_id = str(message.get("from_id") or peer_id)

            # Get attachments from the raw message object
            attachments = message.get("attachments", [])

            inbox_id = getattr(adapters.get("vk"), "inbox_id", None)
            if not inbox_id:
                raise RuntimeError("VK inbox_id is not configured")

            # Enrich with profile
            vk_name: Optional[str] = None
            vk_bdate: Optional[str] = None
            additional_attributes: Dict[str, Any] = {}
            avatar_url: Optional[str] = None

            if config.vk:
                profile = await _fetch_vk_profile(
                    access_token=config.vk.access_token,
                    api_version=config.vk.api_version,
                    user_id=from_id,
                )
                first = (profile.get("first_name") or "").strip()
                last = (profile.get("last_name") or "").strip()
                screen_name = (profile.get("screen_name") or "").strip()
                vk_bdate = (profile.get("bdate") or "").strip() or None
                avatar_url = (profile.get("photo_200") or "")

                # Extract city from profile; VK may return dict with "title" or a plain string
                city_info = profile.get("city")
                city_name: Optional[str] = None
                if isinstance(city_info, dict):
                    city_name = (city_info.get("title") or "").strip() or None
                elif isinstance(city_info, str):
                    city_name = city_info.strip() or None
                if city_name:
                    additional_attributes["city"] = city_name

                if first or last:
                    vk_name = f"{first} {last}".strip()
                elif screen_name:
                    vk_name = screen_name

            custom_attributes = {"vk_user_id": from_id, "vk_peer_id": peer_id}
            if vk_bdate:
                custom_attributes["vk_bdate"] = vk_bdate

            # Process attachments if present
            if attachments:
                logger.info("[events] Processing VK message with %d attachments", len(attachments))
                # Use VK media service to download attachments
                vk_adapter = adapters.get("vk")
                media_service = getattr(vk_adapter, "_media_service", None) if vk_adapter else None
                
                if media_service:
                    try:
                        files_bytes, filenames = await media_service.process_incoming_attachments(attachments)
                        if files_bytes and filenames:
                            await _process_vk_attachments_and_send_to_chatwoot(
                                cw=cw,
                                inbox_id=inbox_id,
                                peer_id=peer_id,
                                from_id=from_id,
                                text=text,
                                files_bytes=files_bytes,
                                filenames=filenames,
                                vk_name=vk_name,
                                custom_attributes=custom_attributes,
                                additional_attributes=additional_attributes,
                                avatar_url=avatar_url,
                            )
                            return
                        else:
                            logger.warning("[events] No files downloaded from attachments, falling back to text-only")
                    except Exception as e:
                        logger.exception("[events] Failed to process attachments: %s", e)
                        # Fallback to text-only on error

            # Handle text-only message
            logger.info("[events] Processing VK text-only message: %s", text[:50] if text else "(empty)")
            ensured = await cw.ensure_contact(
                inbox_id=inbox_id,
                search_key=from_id,
                name=vk_name or from_id,
                phone=None,
                email=None,
                custom_attributes=custom_attributes,
                additional_attributes=additional_attributes,
                avatar_url=avatar_url,
            )

            conv_id = await cw.ensure_conversation(
                inbox_id=inbox_id,
                contact_id=ensured["id"],
                source_id=ensured["source_id"],
            )
            await cw.create_message(
                conversation_id=conv_id,
                content=text,
                direction="incoming",
            )
            logger.info(
                "[events] vk -> chatwoot OK conv_id=%s inbox=%s", conv_id, inbox_id
            )
        except Exception as e:
            logger.exception("[events] vk handling failed: %s", e)

    @bus.on("vk.confirmation")
    async def _vk_confirm(ev: Dict[str, Any]) -> None:
        logger.info("[vk] confirmation acknowledged: group_id=%s", ev.get("group_id"))

    @bus.on("chatwoot.outgoing")
    async def _chatwoot_outgoing(payload: Dict[str, Any]) -> None:
        await router.handle_outgoing(payload)

    @bus.on("telegram.incoming")
    async def _ingest_telegram(payload: Dict[str, Any]) -> None:
        """
        Handle incoming Telegram message and forward it to Chatwoot.
        - Search or upsert contact using telegram_user_id and telegram_username.
        - Ensure conversation by source_id (user_id or username).
        - Create incoming message in Chatwoot.
        """
        try:
            text = (payload.get("text") or "").strip()
            from_id = str(payload.get("from_id") or "")
            username = payload.get("username")
            name = payload.get("name") or username or from_id

            inbox_id = _inbox_from_adapter("telegram")
            if not inbox_id:
                raise RuntimeError("Telegram inbox_id is not configured")

            # Build custom_attributes for Chatwoot contact lookup
            custom_attributes = {}
            if from_id:
                custom_attributes["telegram_user_id"] = from_id
            if username:
                custom_attributes["telegram_username"] = username

            # Use username as search_key if available, else from_id
            search_key = username or from_id

            # Upsert contact in Chatwoot
            contact = await cw.ensure_contact(
                inbox_id=inbox_id,
                search_key=search_key,
                name=name,
                phone=None,
                email=None,
                custom_attributes=custom_attributes,
            )

            # Use source_id returned by ensure_contact (should be user_id or username)
            conv_id = await cw.ensure_conversation(
                inbox_id=inbox_id,
                contact_id=contact["id"],
                source_id=contact["source_id"],
            )

            await cw.create_message(
                conversation_id=conv_id,
                content=text,
                direction="incoming",
            )

            logger.info(
                "[events] telegram -> chatwoot OK conv_id=%s inbox=%s",
                conv_id,
                inbox_id,
            )
        except Exception as e:
            logger.exception("[events] telegram handling failed: %s", e)
