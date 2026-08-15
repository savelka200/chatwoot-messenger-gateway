import logging
from typing import Any, Dict, Mapping, Optional, List, Tuple

import httpx
from pyee.asyncio import AsyncIOEventEmitter

from app.application.chatwoot_service import ChatwootService
from app.application.router import MessageRouter
from app.config import AppConfig
from app.infra.chatwoot_client import ChatwootClient
from app.domain.message import UnifiedMessage, TextContent, MediaContent


logger = logging.getLogger(__name__)
MAX_DOC_SIZE_BYTES = 20 * 1024 * 1024

def _format_size(size_bytes: int) -> str:
    """Человекочитаемый размер файла."""
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024 or unit == "GB":
            return f"{size_bytes:.1f} {unit}" if unit != "B" else f"{size_bytes} B"
        size_bytes /= 1024
    return f"{size_bytes:.1f} GB"


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

    @bus.on("vk.message")
    async def _ingest_vk(umsg: UnifiedMessage) -> None:
        """
        VK (Callback API) incoming:
        - enrich contact with name (first+last; fallback to screen_name) and bdate
        - custom_attributes: vk_user_id, vk_peer_id, vk_bdate (if present)
        - additional_attributes: city (if present in users.get)
        - rely on ensure_contact() to find by /contacts/filter
        """
        try:
            # Извлекаем данные из UnifiedMessage
            from_id = umsg.sender_id
            peer_id = umsg.recipient_id

            # Скачиваем все медиа (фото и видео-превью — оба приходят как картинки по MIME)
            photo_files: List[Tuple[str, bytes, str]] = []
            video_titles: List[str] = []  # собираем заголовки видео для комментария
            doc_mentions: List[str] = []
            voice_mentions: List[str] = []

            for idx, media in enumerate(umsg.attachments):
                # Регистрация для текстовых комментариев
                if media.media_type == "video":
                    title = (media.caption or "без названия").strip()
                    video_titles.append(title)          

                # === Скачивание ===
                try:
                    if media.media_type == "document":
                        size = (media.raw or {}).get("size")
                        if size and size > MAX_DOC_SIZE_BYTES:
                            doc_mentions.append(
                                f"📎 {media.caption} ({_format_size(size)}): {media.url}"
                            )
                            logger.info(
                                "[vk] doc too large (%s bytes), sending link only: %s",
                                size, media.caption,
                            )
                            continue            

                    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as dl:
                        resp = await dl.get(str(media.url))
                        resp.raise_for_status()
                        mime = (resp.headers.get("content-type") or media.mime_type or "application/octet-stream").split(";")[0]
                    photo_files.append((media.filename or f"vk_media_{idx}", resp.content, mime))           

                except httpx.HTTPStatusError as e:
                    status = e.response.status_code
                    if media.media_type == "video":
                        logger.info("[vk] video preview not available (status=%s): %s", status, media.caption)
                    elif media.media_type == "document":
                        doc_mentions.append(f"📎 {media.caption}: {media.url}")
                        logger.warning("[vk] doc download failed (status=%s), sending link: %s", status, media.caption)
                    elif media.media_type == "audio":
                        duration = (media.raw or {}).get("duration", 0)
                        voice_mentions.append(f"🎤 Голосовое сообщение ({duration} сек)")
                        logger.warning("[vk] voice message download failed (status=%s), sending text mention", status)
                    else:
                        logger.warning("[vk] media download failed (%s): %s", media.url, e)
                except Exception as e:
                    if media.media_type == "document":
                        doc_mentions.append(f"📎 {media.caption}: {media.url}")
                        logger.warning("[vk] doc download failed, sending link: %s — %s", media.caption, e)
                    elif media.media_type == "audio":
                        duration = (media.raw or {}).get("duration", 0)
                        voice_mentions.append(f"🎤 Голосовое сообщение ({duration} сек)")
                        logger.warning("[vk] voice message download failed: %s", e)
                    else:
                        logger.warning("[vk] media download failed (%s): %s", media.url, e)         

            # Собираем основной текст сообщения
            text = ""
            if isinstance(umsg.content, TextContent):
                text = umsg.content.text.strip()
            elif isinstance(umsg.content, MediaContent) and umsg.content.caption:
                text = umsg.content.caption.strip()         

            if not text and photo_files and not video_titles and not doc_mentions and not voice_mentions:
                text = "Вложения"           

            # Формируем блоки комментариев
            comment_blocks: List[str] = []          

            if video_titles:
                video_block = "🎬 Пользователь отправил видео, посмотрите его через ВК:\n"
                video_block += "\n".join(f"  • {title}" for title in video_titles)
                comment_blocks.append(video_block)          

            if doc_mentions:
                doc_block = "📎 Документы (ссылки):\n"
                doc_block += "\n".join(f"  {m}" for m in doc_mentions)
                comment_blocks.append(doc_block)            

            if voice_mentions:
                voice_block = "🎤 Голосовые сообщения (не удалось скачать):\n"
                voice_block += "\n".join(f"  {m}" for m in voice_mentions)
                comment_blocks.append(voice_block)          

            if comment_blocks:
                text = text + "\n\n" + "\n\n".join(comment_blocks) if text else "\n\n".join(comment_blocks)


            # Обогащение профиля (как было)
            vk_name: Optional[str] = None
            vk_bdate: Optional[str] = None
            additional_attributes: Dict[str, Any] = {}

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
                photo = (profile.get("photo_200") or "")

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

            inbox_id = getattr(adapters.get("vk"), "inbox_id", None)
            if not inbox_id:
                raise RuntimeError("VK inbox_id is not configured")

            custom_attributes = {"vk_user_id": from_id, "vk_peer_id": peer_id}
            if vk_bdate:
                custom_attributes["vk_bdate"] = vk_bdate

            # Let ensure_contact handle attribute-first lookup
            ensured = await cw.ensure_contact(
                inbox_id=inbox_id,
                search_key=from_id,
                name=vk_name or from_id,
                phone=None,
                email=None,
                custom_attributes=custom_attributes,
                additional_attributes=additional_attributes,
                avatar_url=profile.get("photo_200") if profile else None
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
                attachments=photo_files or None,
            )
            logger.info("[events] vk -> chatwoot OK conv_id=%s inbox=%s", conv_id, inbox_id)
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
