import logging
import httpx
from typing import Any, Dict, List, Optional, Tuple, Union
from pyee.asyncio import AsyncIOEventEmitter

from app.config import OKConfig
from app.domain.message import TextContent, UnifiedMessage, MediaContent
from app.domain.ports import MessengerAdapter

logger = logging.getLogger(__name__)


class OKAdapter(MessengerAdapter):
    """Odnoklassniki adapter for Graph API."""

    def __init__(self, bus: AsyncIOEventEmitter, config: OKConfig):
        self._bus = bus
        self._config = config
        self.inbox_id = config.inbox_id
        self._http: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        """Инициализирует HTTP клиент."""
        if self._http is None:
            self._http = httpx.AsyncClient(
                base_url="https://api.ok.ru/graph",
                timeout=15,
                headers={
                    "Content-Type": "application/json;charset=utf-8",
                },
            )
        logger.info("[ok] adapter started")

    async def stop(self) -> None:
        """Закрывает HTTP клиент."""
        if self._http:
            await self._http.aclose()
            self._http = None
        logger.info("[ok] adapter stopped")

    async def get_user_profile(self, user_id: str, chat_id: str) -> Dict[str, Any]:
        """
        Получает информацию о пользователе через последнее сообщение в чате.
        Костыль, потому что прямого метода для профиля нет в Bot API.

        Args:
            user_id: ID пользователя в формате "user:123456789012"
            chat_id: ID чата в формате "chat:C3ecb9d02a600"

        Returns:
            Dict с полями name, user_id
        """
        if not self._http:
            return {}

        try:
            # Запрашиваем последнее сообщение из чата
            url = f"/{chat_id}/messages?access_token={self._config.access_token}&count=1"
            resp = await self._http.get(url)
            resp.raise_for_status()
            data = resp.json()

            logger.info("[ok] chat messages response: %s", data)

            # Ищем сообщение от нужного пользователя
            messages = data.get("messages", [])
            for msg in messages:
                sender = msg.get("sender", {})
                if sender.get("user_id") == user_id:
                    return {
                        "name": sender.get("name", ""),
                        "user_id": user_id,
                    }

            # Если не нашли в последнем сообщении, возвращаем пустой результат
            logger.warning("[ok] user %s not found in last messages of chat %s", user_id, chat_id)
            return {}

        except Exception as e:
            logger.warning("[ok] failed to fetch user profile via messages: %s", e)
            return {}

    async def subscribe_to_webhook(self, webhook_url: str) -> bool:
        """
        Подписывается на webhook через Graph API.
        Возвращает True при успешной подписке.
        """
        if not self._http:
            raise RuntimeError("OK HTTP client is not initialized")

        try:
            resp = await self._http.post(
                f"/me/subscribe?access_token={self._config.access_token}",
                json={"url": webhook_url},
            )
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("success"):
                logger.info("[ok] successfully subscribed to webhook: %s", webhook_url)
                return True
            else:
                logger.error("[ok] webhook subscription failed: %s", data)
                return False
        except Exception as e:
            logger.exception("[ok] failed to subscribe to webhook: %s", e)
            return False

    async def _ok_call(
        self, method: str, data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Делает запрос к Graph API."""
        if not self._http:
            raise RuntimeError("OK HTTP client is not initialized")

        url = f"/{method}?access_token={self._config.access_token}"
        resp = await self._http.post(url, json=data or {})
        resp.raise_for_status()
        return resp.json()

    async def _download_file(self, url: str) -> bytes:
        """Скачивает файл по URL."""
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.content

    async def get_file_upload_url(self, file_type: str) -> Dict[str, str]:
        """
        Получает URL для загрузки файла и токен (для FILE/VIDEO/AUDIO).

        Returns:
            Dict с полями 'url' и 'token' (token только для FILE/VIDEO/AUDIO)
        """
        if not self._http:
            raise RuntimeError("OK HTTP client is not initialized")

        url = f"/me/fileUploadUrl?access_token={self._config.access_token}&type={file_type}"
        resp = await self._http.get(url)
        resp.raise_for_status()
        data = resp.json()

        upload_url = data.get("url")
        token = data.get("token")  # Токен для FILE/VIDEO/AUDIO
        file_id = data.get("file_id")  # Только для IMAGE

        if not upload_url:
            raise RuntimeError(f"No upload URL in response: {data}")

        logger.info(
            "[ok] got upload URL for %s: url=%s, has_token=%s",
            file_type, upload_url[:80] + "...", bool(token),
        )

        return {
            "url": upload_url,
            "token": token,  # None для IMAGE
            "file_id": file_id,
        }

    async def upload_file(self, upload_url: str, file_bytes: bytes, filename: str, mime_type: str, file_type: str) -> Optional[str]:
        """
        Загружает файл. Для IMAGE возвращает токен из ответа POST.
        Для FILE/VIDEO/AUDIO возвращает None (токен уже получен в get_file_upload_url).
        """
        async with httpx.AsyncClient(timeout=120.0) as client:
            files = {"file": (filename, file_bytes, mime_type)}
            resp = await client.post(upload_url, files=files)
            resp.raise_for_status()

            # Для FILE/VIDEO/AUDIO ответ пустой — это нормально
            if file_type in ("FILE", "VIDEO", "AUDIO"):
                logger.info("[ok] file uploaded (%s), response is empty (expected)", file_type)
                return None

            # Для IMAGE парсим токен из ответа
            try:
                data = resp.json()
                photos = data.get("photos") or {}
                for photo_data in photos.values():
                    if isinstance(photo_data, dict) and "token" in photo_data:
                        token = photo_data["token"]
                        logger.info("[ok] IMAGE uploaded, token: %s...", token[:50])
                        return token
            except Exception as e:
                logger.warning("[ok] failed to parse IMAGE upload response: %s", e)

            raise RuntimeError(f"No token in IMAGE upload response: {resp.text[:200]}")

    async def send_text(self, recipient_id: str, content: TextContent) -> None:
        """Отправляет текстовое сообщение."""
        text = content.text or ""
        if not text:
            logger.info("[ok] skip send: empty text")
            return

        try:
            data = {
                "recipient": {"chat_id": recipient_id},
                "message": {"text": text},
            }
            res = await self._ok_call("me/messages", data)
            logger.info("[ok] SENT: recipient=%s result=%s", recipient_id, res)
        except Exception as e:
            logger.exception("[ok] Failed to send text to %s: %s", recipient_id, e)

    async def send_message(
        self,
        recipient_id: str,
        text: str,
        attachments: List[MediaContent],
        reply_to_message_id: Optional[str] = None,
    ) -> List[str]:
        """
        Отправляет сообщение в ОК с учётом ограничений:
        - Фото группируются (до 5 штук)
        - Документы/видео/аудио — по одному на сообщение
        - Текст прикрепляется к первому сообщению
        - Retry для видео (обрабатывается долго)
        """
        failed_attachments: List[str] = []
        import asyncio

        # === Шаг 1: Группируем вложения ===
        images: List[MediaContent] = []
        singles: List[MediaContent] = []

        for media in attachments:
            if media.media_type == "image":
                images.append(media)
            elif media.media_type in ("document", "video", "audio"):
                singles.append(media)
            else:
                singles.append(media)

        # === Шаг 2: План отправки ===
        messages_to_send: List[Tuple[str, List[MediaContent]]] = []

        if images:
            messages_to_send.append((text or "", images[:5]))
            for i in range(5, len(images), 5):
                messages_to_send.append(("", images[i:i+5]))
            text = ""

        for single in singles:
            messages_to_send.append(("", [single]))

        if not messages_to_send and text:
            messages_to_send.append((text, []))

        if not messages_to_send:
            logger.warning("[ok] nothing to send")
            return failed_attachments

        logger.info(
            "[ok] plan: %d messages (text=%r, images=%d, singles=%d)",
            len(messages_to_send), (text or "")[:30], len(images), len(singles),
        )

        # === Шаг 3: Отправляем каждое сообщение ===
        for msg_idx, (msg_text, msg_attachments) in enumerate(messages_to_send):
            try:
                attachment_payloads: List[Dict[str, Any]] = []
                has_video = False
                total_size = 0

                for media in msg_attachments:
                    file_bytes = await self._download_file(str(media.url))
                    total_size += len(file_bytes)

                    if media.media_type == "image":
                        ok_type = "IMAGE"
                    elif media.media_type == "video":
                        ok_type = "VIDEO"
                        has_video = True
                    elif media.media_type == "document":
                        ok_type = "FILE"
                    elif media.media_type == "audio":
                        ok_type = "AUDIO"
                    else:
                        ok_type = "FILE"

                    logger.info(
                        "[ok] uploading %s: %s (%d bytes) [msg %d/%d]",
                        ok_type, media.filename, len(file_bytes),
                        msg_idx + 1, len(messages_to_send),
                    )

                    upload_info = await self.get_file_upload_url(ok_type)
                    upload_url = upload_info["url"]
                    pre_token = upload_info["token"]

                    upload_token = await self.upload_file(
                        upload_url,
                        file_bytes,
                        media.filename or f"file.{ok_type.lower()}",
                        media.mime_type or "application/octet-stream",
                        ok_type,
                    )

                    final_token = pre_token if ok_type != "IMAGE" else upload_token
                    if not final_token:
                        raise RuntimeError(f"No token available for {ok_type}")

                    attachment_payloads.append({
                        "type": ok_type,
                        "payload": {"token": final_token},
                    })

                # === НОВОЕ: Задержка зависит от типа и размера ===
                if attachment_payloads:
                    if has_video:
                        # Для видео: 5 сек базово + 1 сек на каждый МБ (до 30 сек)
                        size_mb = total_size / (1024 * 1024)
                        delay = min(5.0 + size_mb, 30.0)
                        logger.info("[ok] waiting %.1fs for video processing", delay)
                    else:
                        # Для документов/аудио: 2 сек + 0.5 сек на файл
                        delay = min(2.0 + 0.5 * len(attachment_payloads), 10.0)
                    await asyncio.sleep(delay)

                # Формируем сообщение
                
                message_data: Dict[str, Any] = {}

                # === НОВОЕ: text всегда должен быть (требование ОК API) ===
                if msg_text:
                    message_data["text"] = msg_text
                elif attachment_payloads:
                    # Для одиночных вложений без текста — формируем подпись из имени файла
                    descriptions = []
                    for media in msg_attachments:
                        icon = {
                            "image": "🖼",
                            "video": "🎬",
                            "audio": "🎵",
                            "document": "📎",
                        }.get(media.media_type, "📎")
                        descriptions.append(f"{icon}")
                    message_data["text"] = "\n".join(descriptions)
                else:
                    # Fallback — хотя бы пробел, чтобы ОК не ругался
                    message_data["text"] = " "

                if attachment_payloads:
                    message_data["attachments"] = attachment_payloads

                # === НОВОЕ: Отправляем с retry ===
                await self._send_message_with_retry(
                    recipient_id=recipient_id,
                    message_data=message_data,
                    max_retries=3,
                    base_delay=3.0,  # для видео нужно больше времени
                )

                logger.info(
                    "[ok] SENT message %d/%d: text=%r attachments=%d",
                    msg_idx + 1, len(messages_to_send),
                    (msg_text or "")[:30], len(attachment_payloads),
                )

                # Пауза между сообщениями
                if msg_idx < len(messages_to_send) - 1:
                    await asyncio.sleep(1.0)

            except Exception as e:
                logger.error(
                    "[ok] failed to send message %d/%d: %s",
                    msg_idx + 1, len(messages_to_send), e,
                )
                for media in msg_attachments:
                    failed_attachments.append(f"📎 {media.filename}")
                if msg_idx == 0 and msg_text and not msg_attachments:
                    failed_attachments.append(f"💬 Текст: {msg_text[:50]}...")

        return failed_attachments

    @classmethod
    def parse_ok_media(
        cls, msg: Dict[str, Any]
    ) -> Tuple[Union[TextContent, MediaContent], List[MediaContent]]:
        """
        Парсит сообщение из webhook Одноклассников.
        Поддерживает: IMAGE, VIDEO, FILE, SHARE.
        """
        text = (msg.get("text") or "").strip()
        message_id = msg.get("mid") or "msg"

        attachments: List[MediaContent] = []

        for idx, att in enumerate(msg.get("attachments") or []):
            att_type = att.get("type")
            payload = att.get("payload") or {}

            if att_type == "IMAGE":
                image_url = payload.get("url")
                if not image_url:
                    continue
                attachments.append(
                    MediaContent(
                        type="media",
                        media_type="image",
                        url=image_url,
                        filename=f"ok_{message_id}_{idx}.jpg",
                        mime_type="image/jpeg",
                    )
                )

            elif att_type == "VIDEO":
                video_url = payload.get("url")
                if not video_url:
                    continue
                title = payload.get("title") or f"video_{idx}"
                # Определяем расширение из URL или используем mp4 по умолчанию
                ext = "mp4"
                if ".mp4" in video_url.lower():
                    ext = "mp4"
                elif ".webm" in video_url.lower():
                    ext = "webm"
                elif ".mov" in video_url.lower():
                    ext = "mov"

                attachments.append(
                    MediaContent(
                        type="media",
                        media_type="video",
                        url=video_url,
                        caption=title,
                        filename=f"ok_{message_id}_{idx}.{ext}",
                        mime_type=f"video/{ext}",
                    )
                )

            elif att_type == "FILE":
                file_url = payload.get("url")
                if not file_url:
                    continue
                # Имя файла может быть в payload или формируем из ID
                file_name = payload.get("name") or f"file_{payload.get('id', idx)}"
                # Определяем MIME тип по расширению
                import mimetypes
                mime_type, _ = mimetypes.guess_type(file_name)
                mime_type = mime_type or "application/octet-stream"

                attachments.append(
                    MediaContent(
                        type="media",
                        media_type="document",
                        url=file_url,
                        caption=file_name,
                        filename=file_name,
                        mime_type=mime_type,
                    )
                )

            elif att_type == "SHARE":
                # Ссылка — добавляем в текст, если текста нет
                share_url = payload.get("url")
                if share_url and not text:
                    text = share_url
                elif share_url and text:
                    text = f"{text}\n\n{share_url}"

            elif att_type == "AUDIO":
                audio_url = payload.get("url")
                if not audio_url:
                    continue
                title = payload.get("title") or f"audio_{idx}"
                ext = "mp3"
                if ".ogg" in audio_url.lower():
                    ext = "ogg"
                elif ".wav" in audio_url.lower():
                    ext = "wav"

                attachments.append(
                    MediaContent(
                        type="media",
                        media_type="audio",
                        url=audio_url,
                        caption=title,
                        filename=f"ok_{message_id}_{idx}.{ext}",
                        mime_type=f"audio/{ext}",
                        raw={"duration": payload.get("duration")},
                    )
                )

        if not attachments:
            return TextContent(type="text", text=text), []

        primary: Union[TextContent, MediaContent] = TextContent(type="text", text=text)
        return primary, attachments

    async def _send_message_with_retry(
        self,
        recipient_id: str,
        message_data: Dict[str, Any],
        max_retries: int = 3,
        base_delay: float = 2.0,
    ) -> Dict[str, Any]:
        """
        Отправляет сообщение в ОК с retry при ошибках обработки.

        Returns:
            Ответ от API или пустой dict при полном провале.
        """
        import asyncio

        last_error = None
        for attempt in range(max_retries):
            try:
                data = {
                    "recipient": {"chat_id": recipient_id},
                    "message": message_data,
                }

                response = await self._ok_call("me/messages", data)

                # === НОВОЕ: Логируем полный ответ ===
                logger.info(
                    "[ok] /me/messages response (attempt %d/%d): %s",
                    attempt + 1, max_retries, response,
                )

                # Проверяем, что сообщение действительно создано
                # ОК может вернуть 200, но с ошибкой внутри
                if isinstance(response, dict):
                    # Если есть message_id — всё хорошо
                    if "message_id" in response or "mid" in response:
                        return response

                    # Если есть error — это проблема
                    if "error_code" in response or "error_msg" in response:
                        error_msg = response.get("error_msg", "")
                        error_code = response.get("error_code")
                        logger.warning(
                            "[ok] /me/messages returned error: code=%s msg=%s",
                            error_code, error_msg,
                        )
                        # Если видео ещё обрабатывается — повторяем
                        if "processing" in error_msg.lower() or "not ready" in error_msg.lower():
                            delay = base_delay * (2 ** attempt)
                            logger.info("[ok] video still processing, retry in %ss", delay)
                            await asyncio.sleep(delay)
                            last_error = error_msg
                            continue
                        # Другая ошибка — не повторяем
                        raise RuntimeError(f"OK API error {error_code}: {error_msg}")

                # Ответ без явного message_id, но и без ошибки
                # Возможно, ОК просто не вернул ID — считаем успешным
                return response or {}

            except Exception as e:
                last_error = str(e)
                logger.warning(
                    "[ok] /me/messages failed (attempt %d/%d): %s",
                    attempt + 1, max_retries, e,
                )
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    await asyncio.sleep(delay)

        raise RuntimeError(f"Failed to send message after {max_retries} attempts: {last_error}")