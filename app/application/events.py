import logging
from typing import Any, Dict, Mapping, Optional, List, Tuple

import httpx
from pyee.asyncio import AsyncIOEventEmitter

from app.application.chatwoot_service import ChatwootService
from app.application.router import MessageRouter
from app.config import AppConfig
from app.infra.chatwoot_client import ChatwootClient
from app.domain.message import UnifiedMessage, TextContent, MediaContent

from contextlib import asynccontextmanager
from app.infra.adapters.ok_bot import OKAdapter
from app.infra.adapters.max_bot import MaxAdapter

import re
from urllib.parse import unquote


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

    @bus.on("ok.incoming")
    async def _ingest_ok(payload: Dict[str, Any]) -> None:
        """Обрабатывает входящее сообщение из Одноклассников."""
        try:
            sender_raw = payload.get("sender", {}).get("user_id", "")
            chat_id = payload.get("recipient", {}).get("chat_id", "")
            msg = payload.get("message") or {}

            # Извлекаем user_id из строки "user:123456789012"
            user_id = sender_raw.split(":")[-1] if ":" in sender_raw else sender_raw

            # Парсим текст и вложения
            content, attachments = OKAdapter.parse_ok_media(msg)

            # Карта MIME → расширение для добавления к именам без расширения
            MIME_TO_EXT = {
                "application/pdf": "pdf",
                "application/msword": "doc",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
                "application/vnd.ms-excel": "xls",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
                "application/vnd.ms-powerpoint": "ppt",
                "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
                "application/zip": "zip",
                "application/x-rar-compressed": "rar",
                "application/x-7z-compressed": "7z",
                "application/octet-stream": None,  # универсальный, не добавляем
                "video/mp4": "mp4",
                "video/webm": "webm",
                "video/quicktime": "mov",
                "video/x-msvideo": "avi",
                "audio/mpeg": "mp3",
                "audio/ogg": "ogg",
                "audio/wav": "wav",
                "audio/x-wav": "wav",
                "text/plain": "txt",
            }

            # Скачиваем все медиа
            media_files: List[Tuple[str, bytes, str]] = []
            video_links_for_text: List[str] = []  # ссылки на плееры, которые не удалось извлечь как файл

            for idx, media in enumerate(attachments):
                try:
                    # === Специальная обработка для видео ===
                    if media.media_type == "video":
                        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as dl:
                            resp = await dl.get(str(media.url))
                            resp.raise_for_status()

                            content_type = (resp.headers.get("content-type") or "").split(";")[0].strip()
                            logger.info("[ok] video response: content-type=%s, length=%d", content_type, len(resp.content))

                            # Если это HTML-страница (content-type text/html), ищем прямую ссылку
                            if "text/html" in content_type or "ok.ru/video/" in str(media.url):
                                html_content = resp.text
                                direct_url = OKAdapter.extract_direct_video_url(html_content)

                                if direct_url:
                                    logger.info("[ok] found direct video URL: %s", direct_url[:100])
                                    # Скачиваем видео по прямой ссылке
                                    video_resp = await dl.get(direct_url)
                                    video_resp.raise_for_status()

                                    # Определяем имя и расширение
                                    video_filename = media.caption or f"ok_video_{idx}.mp4"
                                    if not video_filename.lower().endswith(".mp4"):
                                        video_filename += ".mp4"

                                    mime = (video_resp.headers.get("content-type") or "video/mp4").split(";")[0].strip()
                                    media_files.append((video_filename, video_resp.content, mime))
                                    logger.info("[ok] video downloaded: %s (%d bytes)", video_filename, len(video_resp.content))
                                else:
                                    # Не удалось извлечь прямую ссылку — добавляем ссылку на плеер в текст
                                    logger.warning("[ok] could not extract direct video URL, adding link to text")
                                    video_links_for_text.append(
                                        f"🎬 Видео: {media.caption or 'Без названия'} — {media.url}"
                                    )
                            else:
                                # Это прямой видеофайл — используем как есть
                                video_filename = media.caption or f"ok_video_{idx}.mp4"
                                if "." not in video_filename.rsplit("/", 1)[-1]:
                                    video_filename += ".mp4"
                                media_files.append((video_filename, resp.content, content_type or "video/mp4"))

                        continue
                    
                    # === Обычная обработка для остальных типов (документы, фото, аудио) ===
                    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as dl:
                        resp = await dl.get(str(media.url))
                        resp.raise_for_status()

                        filename = media.filename
                        mime_from_header = (resp.headers.get("content-type") or "").split(";")[0].strip()

                        if media.media_type in ("document", "audio"):
                            # Извлекаем имя из Content-Disposition
                            content_disp = resp.headers.get("content-disposition", "")
                            if content_disp:
                                match_utf8 = re.search(r"filename\*\s*=\s*(?:UTF-8''|utf-8'')([^;\s]+)", content_disp, re.I)
                                match_plain = re.search(r'filename\s*=\s*"?([^";\s]+)"?', content_disp, re.I)

                                if match_utf8:
                                    filename = unquote(match_utf8.group(1).strip())
                                    logger.info("[ok] extracted filename (UTF-8): %s", filename)
                                elif match_plain:
                                    filename = unquote(match_plain.group(1).strip())
                                    logger.info("[ok] extracted filename: %s", filename)

                            if filename and "." not in filename.rsplit("/", 1)[-1] and mime_from_header:
                                ext = MIME_TO_EXT.get(mime_from_header)
                                if ext:
                                    filename = f"{filename}.{ext}"
                                    logger.info("[ok] added extension .%s", ext)

                        mime = mime_from_header or media.mime_type or "application/octet-stream"

                    media_files.append((filename or f"ok_media_{idx}", resp.content, mime))

                except Exception as e:
                    logger.warning("[ok] media download failed (%s): %s", media.url, e)

            # === Формирование финального текста ===

            # 1. Базовый текст сообщения от пользователя
            base_text = ""
            if isinstance(content, TextContent):
                base_text = content.text.strip()

            # 2. Блок с ссылками на видео (если не удалось скачать)
            video_block = ""
            if video_links_for_text:
                video_block = "📹 Пользователь прислал видео (откройте в ОК)\n"

            # 3. Объединяем
            if base_text and video_block:
                final_text = f"{base_text}\n\n{video_block}"
            elif base_text:
                final_text = base_text
            elif video_block:
                final_text = video_block
            elif media_files:
                final_text = "Медиа"
            else:
                final_text = ""

            # Логируем, что получилось
            logger.info(
                "[ok] final message: text=%r media_files=%d video_links=%d",
                final_text[:100] if final_text else "",
                len(media_files),
                len(video_links_for_text),
            )

            # === ПОЛУЧЕНИЕ ПРОФИЛЯ ПОЛЬЗОВАТЕЛЯ ===
            ok_adapter = adapters.get("ok")
            profile = {}
            ok_name = f"OK User {user_id}"
            avatar_url = None

            if ok_adapter:
                # Передаём chat_id для запроса сообщений
                profile = await ok_adapter.get_user_profile(sender_raw, chat_id)
                if profile.get("name"):
                    ok_name = profile["name"]
                    logger.info("[ok] got user name from messages: %s", ok_name)

            inbox_id = getattr(ok_adapter, "inbox_id", None) if ok_adapter else None
            if not inbox_id:
                raise RuntimeError("OK inbox_id is not configured")

            custom_attributes = {
                "ok_user_id": sender_raw,
                "ok_chat_id": chat_id,
            }
            additional_attributes = {}
            if profile:
                if profile.get("city"):
                    additional_attributes["city"] = profile["city"]
                if profile.get("birthday"):
                    additional_attributes["birthday"] = profile["birthday"]

            ensured = await cw.ensure_contact(
                inbox_id=inbox_id,
                search_key=user_id,
                name=ok_name,
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
                content=final_text,
                direction="incoming",
                attachments=media_files or None,
            )
            logger.info(
                "[events] ok -> chatwoot OK conv_id=%s inbox=%s name=%s",
                conv_id, inbox_id, ok_name,
            )
        except Exception as e:
            logger.exception("[events] ok handling failed: %s", e)


    @bus.on("max.incoming")
    async def _ingest_max(payload: Dict[str, Any]) -> None:
        """Обрабатывает входящее сообщение из MAX."""
        try:
            # Извлекаем данные
            message = payload.get("message") or payload
            sender = message.get("sender", {})
            recipient = message.get("recipient", {})

            # === ВАЖНО: для диалогов используем sender.user_id как получателя ===
            chat_type = recipient.get("chat_type", "")
            chat_id = recipient.get("chat_id")
            sender_user_id = sender.get("user_id")

            if not sender_user_id:
                logger.warning("[max] missing sender.user_id")
                return

            # Для диалогов: получатель = sender (мы отправляем обратно пользователю)
            # Для групповых чатов: получатель = chat_id
            if chat_type == "dialog":
                # В диалоге используем sender.user_id как recipient для отправки
                max_recipient_id = str(sender_user_id)
                max_recipient_type = "user_id"
            else:
                # В групповом чате используем chat_id
                if not chat_id:
                    logger.warning("[max] missing chat_id for group chat")
                    return
                max_recipient_id = str(chat_id)
                max_recipient_type = "chat_id"

            logger.info(
                "[max] routing: chat_type=%s, recipient_id=%s, recipient_type=%s",
                chat_type, max_recipient_id, max_recipient_type,
            )

            # Парсим сообщение
            text, attachments, reply_to_mid = MaxAdapter.parse_max_message(message)

            # === Обработка reply ===
            # TODO: для reply нужен маппинг message_id -> max_message_id
            # Пока пропускаем, можно добавить позже

            # === Скачиваем медиа ===
            max_adapter = adapters.get("max")
            media_files: List[Tuple[str, bytes, str]] = []
            
            for idx, media in enumerate(attachments):
                try:
                    if not max_adapter:
                        logger.warning("[max] no adapter for download")
                        continue
                    
                    logger.info(
                        "[max] downloading %s: %s (%s)",
                        media.media_type, media.filename, media.url[:100],
                    )
                    
                    # Скачиваем с редиректами и увеличенным timeout
                    # URL видео/фото MAX могут быть на разных CDN
                    import httpx
                    async with httpx.AsyncClient(
                        timeout=60.0,
                        follow_redirects=True,
                    ) as download_client:
                        resp = await download_client.get(str(media.url))
                        resp.raise_for_status()
                        
                        file_bytes = resp.content
                        mime = (resp.headers.get("content-type") or media.mime_type or "application/octet-stream").split(";")[0].strip()
                        
                        # Проверяем, что скачали реальные данные, а не HTML ошибку
                        if len(file_bytes) < 100 and b"<html" in file_bytes.lower():
                            logger.error(
                                "[max] got HTML instead of media for %s: %s",
                                media.filename, file_bytes[:200],
                            )
                            continue
                        
                        logger.info(
                            "[max] downloaded %s: %d bytes, mime=%s",
                            media.filename, len(file_bytes), mime,
                        )
                        
                        media_files.append((media.filename or f"max_media_{idx}", file_bytes, mime))
                
                except Exception as e:
                    logger.warning("[max] media download failed for %s: %s", media.filename, e)
            
            logger.info("[max] total media files to send: %d", len(media_files))

            # === Получаем имя пользователя прямо из webhook ===
            max_name = f"MAX User {sender_user_id}"
            avatar_url = None

            # Извлекаем имя из sender
            if sender:
                first_name = sender.get("first_name") or ""
                last_name = sender.get("last_name") or ""
                name = sender.get("name") or ""
                username = sender.get("username") or ""

                if first_name and last_name:
                    max_name = f"{first_name} {last_name}".strip()
                elif first_name:
                    max_name = first_name
                elif name:
                    max_name = name
                elif username:
                    max_name = username

                # Аватар (если есть в webhook)
                avatar_url = sender.get("avatar") or sender.get("avatar_url") or sender.get("photo_url")

                logger.info("[max] resolved name: %s", max_name)

            # === Обогащаем контакт ===
            inbox_id = getattr(max_adapter, "inbox_id", None) if max_adapter else None
            if not inbox_id:
                raise RuntimeError("MAX inbox_id not configured")

            # Формируем identifier для поиска (max:{user_id})
            max_identifier = f"max:{sender_user_id}"

            custom_attributes = {
                "max_user_id": str(sender_user_id),
                "max_chat_id": str(chat_id) if chat_id else "",
                "max_recipient_type": max_recipient_type,
                "max_recipient_id": max_recipient_id,
            }

            # Используем max_user_id как search_key
            ensured = await cw.ensure_contact(
                inbox_id=inbox_id,
                search_key=str(sender_user_id),
                name=max_name,
                phone=None,
                email=None,
                custom_attributes=custom_attributes,
                avatar_url=avatar_url,
            )

            conv_id = await cw.ensure_conversation(
                inbox_id=inbox_id,
                contact_id=ensured["id"],
                source_id=ensured["source_id"],
            )

            await cw.create_message(
                conversation_id=conv_id,
                content=text or "",
                direction="incoming",
                attachments=media_files or None,
            )

            logger.info(
                "[events] max -> chatwoot OK conv_id=%s inbox=%s name=%s",
                conv_id, inbox_id, max_name,
            )
        except Exception as e:
            logger.exception("[events] max handling failed: %s", e)