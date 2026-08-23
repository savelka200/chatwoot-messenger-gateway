import logging
import httpx
import secrets
import json
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlencode
from pyee.asyncio import AsyncIOEventEmitter

from app.config import MaxConfig
from app.domain.message import TextContent, MediaContent
from app.domain.ports import MessengerAdapter

logger = logging.getLogger(__name__)


class MaxAdapter(MessengerAdapter):
    """MAX messenger adapter."""

    BASE_URL = "https://platform-api2.max.ru"

    def __init__(self, bus: AsyncIOEventEmitter, config: MaxConfig):
        self._bus = bus
        self._config = config
        self.inbox_id = config.inbox_id
        self._http: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        if self._http is None:
            self._http = httpx.AsyncClient(
                base_url=self.BASE_URL,
                timeout=30.0,
                headers={
                    "Authorization": self._config.access_token,
                    "Content-Type": "application/json",
                },
            )
        
        # Получаем bot_id, если не указан
        if not self._config.bot_id:
            try:
                me = await self._api_call("GET", "/me")
                self._config.bot_id = str(me.get("user_id", ""))
                logger.info("[max] bot_id resolved: %s", self._config.bot_id)
            except Exception as e:
                logger.warning("[max] failed to get bot_id from /me: %s", e)
        
        logger.info("[max] adapter started")

    async def stop(self) -> None:
        if self._http:
            await self._http.aclose()
            self._http = None
        logger.info("[max] adapter stopped")

    async def _api_call(
        self, method: str, path: str, data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Универсальный вызов MAX API с подробным логированием."""
        if not self._http:
            raise RuntimeError("MAX HTTP client is not initialized")
        
        # Логируем запрос
        if data:
            logger.info("[max] API %s %s request: %s", method, path, json.dumps(data, ensure_ascii=False))
        else:
            logger.info("[max] API %s %s", method, path)
        
        try:
            if method == "GET":
                resp = await self._http.get(path)
            elif method == "POST":
                resp = await self._http.post(path, json=data or {})
            elif method == "PUT":
                resp = await self._http.put(path, json=data or {})
            elif method == "PATCH":
                resp = await self._http.patch(path, json=data or {})
            elif method == "DELETE":
                resp = await self._http.delete(path)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            # Логируем статус
            logger.info("[max] API %s %s response: status=%d", method, path, resp.status_code)
            
            # При ошибке логируем тело ответа
            if resp.status_code >= 400:
                error_body = resp.text
                logger.error("[max] API error body: %s", error_body)
            
            resp.raise_for_status()
            
            # Некоторые методы (например, uploads) могут вернуть пустой ответ
            if not resp.text:
                return {}
            
            result = resp.json()
            # Логируем ответ (сокращённо, если большой)
            result_str = json.dumps(result, ensure_ascii=False)
            if len(result_str) > 500:
                logger.info("[max] API response (truncated): %s...", result_str[:500])
            else:
                logger.info("[max] API response: %s", result_str)
            
            return result
            
        except httpx.HTTPStatusError as e:
            # Логируем тело ошибки перед исключением
            logger.error("[max] HTTP error: status=%d body=%s", e.response.status_code, e.response.text)
        raise

    async def _download_file(self, url: str) -> bytes:
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.content

    async def subscribe_webhook(self, webhook_url: str, update_types: List[str] = None) -> bool:
        """Подписывается на webhook через POST /subscriptions."""
        if update_types is None:
            update_types = [
                "message_created",
                "message_edited",
                "message_removed",
                "bot_added",
                "bot_started",
            ]
        
        try:
            data = {
                "url": webhook_url,
                "update_types": update_types,
                "secret": self._config.webhook_secret,
            }
            resp = await self._api_call("POST", "/subscriptions", data)
            logger.info("[max] subscribed to webhook: %s", webhook_url)
            return True
        except Exception as e:
            logger.exception("[max] webhook subscription failed: %s", e)
            return False

    async def upload_file(self, file_bytes: bytes, filename: str, file_type: str) -> str:
        """
        Загружает файл через POST /uploads.

        Особенности MAX API:
        - Для image: token приходит в ответе CDN (photos[id].token)
        - Для video/file/audio: token приходит в ответе /uploads
        """
        if not self._http:
            raise RuntimeError("MAX HTTP client is not initialized")

        # === Шаг 1: Получаем URL загрузки (type в query-параметрах!) ===
        logger.info("[max] getting upload URL for type=%s", file_type)

        resp = await self._http.post("/uploads", params={"type": file_type})

        if resp.status_code >= 400:
            logger.error("[max] upload URL error body: %s", resp.text)
        resp.raise_for_status()

        upload_info = resp.json()
        logger.info("[max] upload info: %s", json.dumps(upload_info, ensure_ascii=False)[:500])

        upload_url = upload_info.get("url")
        if not upload_url:
            raise RuntimeError(f"No upload URL in response: {upload_info}")

        # Попробуем получить токен из ответа /uploads (для video/file/audio)
        token = upload_info.get("token")
        if token:
            logger.info("[max] token obtained from /uploads response")

        # === Шаг 2: Загружаем файл на CDN ===
        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as upload_client:
            import mimetypes
            mime_type, _ = mimetypes.guess_type(filename)
            mime_type = mime_type or "application/octet-stream"

            files = {"file": (filename, file_bytes, mime_type)}

            logger.info(
                "[max] uploading %s: %s (%d bytes) to CDN",
                file_type, filename, len(file_bytes),
            )

            upload_resp = await upload_client.post(upload_url, files=files)
            upload_resp.raise_for_status()

            # Парсим ответ CDN
            cdn_data: Dict[str, Any] = {}
            if upload_resp.text.strip():
                try:
                    cdn_data = upload_resp.json()
                    logger.info("[max] CDN response: %s", json.dumps(cdn_data, ensure_ascii=False)[:500])
                except Exception:
                    logger.info("[max] CDN response is not JSON: %s", upload_resp.text[:200])
            else:
                logger.info("[max] CDN response is empty (expected for video/file)")

            # === Извлечение токена в зависимости от типа файла ===

            # Для image: токен в cdn_data.photos[id].token
            if file_type == "image" and not token:
                photos = cdn_data.get("photos") or {}
                for photo_id, photo_data in photos.items():
                    if isinstance(photo_data, dict) and "token" in photo_data:
                        token = photo_data["token"]
                        logger.info("[max] token obtained from CDN photos[%s].token", photo_id[:30])
                        break
                    
            # Fallback: пробуем разные варианты извлечения токена из CDN
            if not token:
                # Прямое поле token
                token = cdn_data.get("token")

                # videos[id].token
                if not token:
                    for video_data in (cdn_data.get("videos") or {}).values():
                        if isinstance(video_data, dict) and "token" in video_data:
                            token = video_data["token"]
                            break
                        
                # files[id].token
                if not token:
                    for file_data in (cdn_data.get("files") or {}).values():
                        if isinstance(file_data, dict) and "token" in file_data:
                            token = file_data["token"]
                            break
                        
                # audios[id].token
                if not token:
                    for audio_data in (cdn_data.get("audios") or {}).values():
                        if isinstance(audio_data, dict) and "token" in audio_data:
                            token = audio_data["token"]
                            break
                        
            if not token:
                raise RuntimeError(
                    f"No token in upload responses. "
                    f"upload_info: {upload_info}, cdn_data: {cdn_data}"
                )

            logger.info("[max] file uploaded (%s), token: %s...", file_type, token[:30])
            return token

    # === Парсер входящих сообщений ===

    @classmethod
    def parse_max_message(
        cls, msg: Dict[str, Any]
    ) -> Tuple[Optional[str], List[MediaContent], Optional[str]]:
        """
        Парсит сообщение из webhook MAX.

        Returns:
            (text, attachments, reply_to_message_id)
        """
        body = msg.get("body") or {}
        text = (body.get("text") or "").strip()
        message_id = body.get("mid") or msg.get("message_id") or msg.get("id") or "msg"

        attachments: List[MediaContent] = []
        # === ВАЖНО: attachments в MAX лежат в body.attachments ===
        attachments_raw = body.get("attachments") or []

        logger.info("[max] parsing %d attachments", len(attachments_raw))

        for idx, att in enumerate(attachments_raw):
            att_type = att.get("type")
            payload = att.get("payload") or {}

            logger.info("[max] attachment %d: type=%s, payload_keys=%s", 
                       idx, att_type, list(payload.keys()))

            if att_type == "image":
                url = payload.get("url")
                if not url:
                    logger.warning("[max] image has no url, skipping")
                    continue
                
                # Имя файла
                filename = f"max_image_{message_id}_{idx}.jpg"

                attachments.append(
                    MediaContent(
                        type="media",
                        media_type="image",
                        url=url,
                        filename=filename,
                        mime_type="image/jpeg",
                        raw={"photo_id": payload.get("photo_id")},
                    )
                )

            elif att_type == "video":
                url = payload.get("url")
                thumbnail = att.get("thumbnail") or {}
                thumbnail_url = thumbnail.get("url")
                duration = att.get("duration")

                if not url:
                    logger.warning("[max] video has no url, skipping")
                    continue
                
                # Имя файла
                filename = f"max_video_{message_id}_{idx}.mp4"

                attachments.append(
                    MediaContent(
                        type="media",
                        media_type="video",
                        url=url,
                        caption=thumbnail_url,  # превью в caption для отладки
                        filename=filename,
                        mime_type="video/mp4",
                        raw={
                            "duration": duration,
                            "video_id": payload.get("id"),
                            "thumbnail_url": thumbnail_url,
                        },
                    )
                )

            elif att_type == "audio":
                url = payload.get("url")
                if not url:
                    continue
                filename = f"max_audio_{message_id}_{idx}.mp3"
                attachments.append(
                    MediaContent(
                        type="media",
                        media_type="audio",
                        url=url,
                        filename=filename,
                        mime_type="audio/mpeg",
                        raw={"duration": payload.get("duration")},
                    )
                )

            elif att_type == "file":
                url = payload.get("url")
                if not url:
                    logger.warning("[max] file has no url, skipping")
                    continue
                
                # === ВАЖНО: filename на корневом уровне аттачмента, не в payload! ===
                filename = att.get("filename") or payload.get("name") or f"file_{idx}"
                
                # Получаем размер
                size = att.get("size") or payload.get("size")
                
                # Определяем MIME-тип по расширению, если не указан
                import mimetypes
                mime_type = payload.get("mime_type")
                if not mime_type and filename:
                    guessed_mime, _ = mimetypes.guess_type(filename)
                    mime_type = guessed_mime or "application/octet-stream"
                
                logger.info("[max] file attachment: filename=%s, size=%s, mime=%s", 
                        filename, size, mime_type)
                
                attachments.append(
                    MediaContent(
                        type="media",
                        media_type="document",
                        url=url,
                        filename=filename,
                        mime_type=mime_type,
                        raw={
                            "size": size,
                            "fileId": payload.get("fileId"),
                        },
                    )
    )

            else:
                logger.warning("[max] unknown attachment type: %s", att_type)

        # Парсим reply (если есть)
        reply_to = msg.get("reply_to")
        if reply_to and isinstance(reply_to, dict):
            reply_to = reply_to.get("message_id")

        return text, attachments, reply_to

    # === Отправка сообщений ===

    async def send_text(self, recipient_id: str, content: TextContent) -> None:
        """Отправляет текстовое сообщение."""
        text = content.text or ""
        if not text:
            return
        
        try:
            await self._api_call(
                "POST",
                "/messages",
                {
                    "recipient": {"chat_id": int(recipient_id)},
                    "body": {"text": text},
                },
            )
            logger.info("[max] SENT text to %s", recipient_id)
        except Exception as e:
            logger.exception("[max] failed to send text: %s", e)

    async def send_message(
        self,
        recipient_id: str,
        text: str,
        attachments: List[MediaContent],
        reply_to_message_id: Optional[str] = None,
        recipient_type: str = "chat_id",
    ) -> List[str]:
        """
        Отправляет сообщение в MAX с учётом ограничений API:
        - image/video/audio: до 12 в одном сообщении
        - file: ТОЛЬКО ПО ОДНОМУ на сообщение (с кнопками или без)
        - После загрузки видео нужна задержка (сервер обрабатывает)
        """
        import asyncio
        
        failed_attachments: List[str] = []
        
        # === Группируем вложения ===
        media_group: List[MediaContent] = []  # image, video, audio — вместе
        file_group: List[MediaContent] = []   # document — каждое отдельно
        
        for media in attachments:
            if media.media_type in ("image", "video", "audio"):
                media_group.append(media)
            else:
                file_group.append(media)
        
        # === Формируем список сообщений ===
        messages_to_send: List[Tuple[str, List[MediaContent]]] = []
        
        # 1. Первое сообщение: текст + медиа (до 12 штук)
        if media_group:
            messages_to_send.append((text or "", media_group[:12]))
            # Если медиа больше 12 — дробим
            for i in range(12, len(media_group), 12):
                messages_to_send.append(("", media_group[i:i+12]))
            text = ""  # текст уже добавлен к первому
        
        # 2. Каждое file — отдельное сообщение (требование API)
        for file_media in file_group:
            messages_to_send.append(("", [file_media]))
        
        # 3. Если нет вложений, но есть текст
        if not messages_to_send and text:
            messages_to_send.append((text, []))
        
        if not messages_to_send:
            return failed_attachments
        
        # Определяем, есть ли в сообщениях видео (нужна задержка)
        has_video = any(
            m.media_type == "video"
            for _, atts in messages_to_send
            for m in atts
        )
        
        logger.info(
            "[max] plan: %d messages (text=%r, media=%d, files=%d, has_video=%s, type=%s)",
            len(messages_to_send), (text or "")[:30],
            len(media_group), len(file_group), has_video, recipient_type,
        )
        
        # === Отправляем каждое сообщение ===
        for msg_idx, (msg_text, msg_attachments) in enumerate(messages_to_send):
            try:
                # Загружаем все вложения
                attachment_payloads: List[Dict[str, Any]] = []
                has_video_in_this_msg = False
                
                for media in msg_attachments:
                    file_bytes = await self._download_file(str(media.url))
                    
                    if media.media_type == "image":
                        max_type = "image"
                    elif media.media_type == "video":
                        max_type = "video"
                        has_video_in_this_msg = True
                    elif media.media_type == "audio":
                        max_type = "audio"
                    else:
                        max_type = "file"
                    
                    logger.info(
                        "[max] uploading %s: %s (%d bytes)",
                        max_type, media.filename, len(file_bytes),
                    )
                    
                    token = await self.upload_file(
                        file_bytes,
                        media.filename or f"file.{max_type}",
                        max_type,
                    )
                    
                    attachment_payloads.append({
                        "type": max_type,
                        "payload": {"token": token},
                    })
                
                # Формируем тело сообщения
                body: Dict[str, Any] = {}
                if msg_text:
                    body["text"] = msg_text
                    body["format"] = "markdown"
                
                # Формируем recipient
                query_params: Dict[str, Any] = {}
                if recipient_type == "user_id":
                    query_params["user_id"] = int(recipient_id)
                else:
                    query_params["chat_id"] = int(recipient_id)
                
                request_data: Dict[str, Any] = dict(body)
                if attachment_payloads:
                    request_data["attachments"] = attachment_payloads
                
                logger.info(
                    "[max] API POST /messages request: params=%s, attachments=%d",
                    json.dumps(query_params, ensure_ascii=False),
                    len(attachment_payloads),
                )
                
                # === ВАЖНО: для видео делаем задержку перед отправкой ===
                # MAX требует время на обработку загруженного видео
                if has_video_in_this_msg:
                    # Базовая задержка 3 секунды на обработку
                    await asyncio.sleep(3.0)
                
                # === Отправка с retry на случай "attachment.not.ready" ===
                max_attempts = 5
                last_error = None
                
                for attempt in range(max_attempts):
                    try:
                        if not self._http:
                            raise RuntimeError("MAX HTTP client is not initialized")
                        
                        resp = await self._http.post(
                            "/messages",
                            params=query_params,
                            json=request_data,
                        )
                        
                        logger.info(
                            "[max] API POST /messages response: status=%d (attempt %d/%d)",
                            resp.status_code, attempt + 1, max_attempts,
                        )
                        
                        if resp.status_code >= 400:
                            error_body = resp.text
                            logger.error("[max] API error body: %s", error_body)
                            
                            # Проверяем, это ли ошибка "not ready"
                            if "not.ready" in error_body or "not.processed" in error_body:
                                if attempt < max_attempts - 1:
                                    delay = 3 * (attempt + 1)
                                    logger.info(
                                        "[max] attachment not ready, retry in %ds",
                                        delay,
                                    )
                                    await asyncio.sleep(delay)
                                    continue
                                
                            # Другая ошибка — пробрасываем
                            resp.raise_for_status()
                        
                        # Успех
                        break
                    
                    except httpx.HTTPStatusError as e:
                        error_body = e.response.text
                        if "not.ready" in error_body or "not.processed" in error_body:
                            if attempt < max_attempts - 1:
                                delay = 3 * (attempt + 1)
                                logger.info(
                                    "[max] attachment not ready, retry in %ds",
                                    delay,
                                )
                                await asyncio.sleep(delay)
                                continue
                        raise
                    
                logger.info(
                    "[max] SENT message %d/%d: text=%r attachments=%d",
                    msg_idx + 1, len(messages_to_send),
                    msg_text[:30] if msg_text else "",
                    len(attachment_payloads),
                )
                
                # Пауза между сообщениями (лимит 2 msg/sec)
                if msg_idx < len(messages_to_send) - 1:
                    await asyncio.sleep(0.6)
            
            except Exception as e:
                logger.error(
                    "[max] failed to send message %d/%d: %s",
                    msg_idx + 1, len(messages_to_send), e,
                )
                for media in msg_attachments:
                    failed_attachments.append(f"📎 {media.filename}")
        
        return failed_attachments