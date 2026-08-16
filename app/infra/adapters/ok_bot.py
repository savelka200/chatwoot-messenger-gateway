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

    async def get_file_upload_url(self, file_type: str) -> str:
        """
        Получает URL для загрузки файла.

        Args:
            file_type: "IMAGE", "VIDEO", "AUDIO", "FILE"

        Returns:
            URL для загрузки файла
        """
        if not self._http:
            raise RuntimeError("OK HTTP client is not initialized")

        url = f"/me/fileUploadUrl?access_token={self._config.access_token}&type={file_type}"
        resp = await self._http.get(url)
        resp.raise_for_status()
        data = resp.json()

        upload_url = data.get("url")
        if not upload_url:
            raise RuntimeError(f"No upload URL in response: {data}")

        logger.info("[ok] got upload URL for %s: %s", file_type, upload_url)
        return upload_url

    async def upload_file(self, upload_url: str, file_bytes: bytes, filename: str, mime_type: str) -> str:
        """
        Загружает файл на полученный URL и возвращает токен.
        ОК возвращает токен в формате: {"photos": {"<id>": {"token": "..."}}}
        """
        async with httpx.AsyncClient(timeout=120.0) as client:
            files = {"file": (filename, file_bytes, mime_type)}
            resp = await client.post(upload_url, files=files)
            resp.raise_for_status()
            data = resp.json()
    
            # Токен может быть в разных местах в зависимости от типа файла
            token = data.get("token")
            
            # Для изображений токен находится в photos[id].token
            if not token:
                photos = data.get("photos") or {}
                for photo_id, photo_data in photos.items():
                    token = photo_data.get("token")
                    if token:
                        break
                    
            # Для видео/аудио/файлов может быть в других полях
            if not token:
                token = data.get("file") or data.get("video", {}).get("token") or data.get("audio", {}).get("token")
            
            if not token:
                raise RuntimeError(f"No token in upload response: {data}")
    
            logger.info("[ok] file uploaded, token: %s", token[:50] + "...")
            return token

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
        """Отправляет сообщение с вложениями в ОК."""
        failed_attachments: List[str] = []
        attachment_payloads: List[Dict[str, Any]] = []

        # Загружаем каждое вложение
        for media in attachments:
            try:
                file_bytes = await self._download_file(str(media.url))

                # Определяем тип файла для ОК
                if media.media_type == "image":
                    ok_type = "IMAGE"
                elif media.media_type == "video":
                    ok_type = "VIDEO"
                elif media.media_type == "audio":
                    ok_type = "AUDIO"
                else:
                    ok_type = "FILE"

                # Получаем URL загрузки
                upload_url = await self.get_file_upload_url(ok_type)

                # Загружаем файл
                token = await self.upload_file(
                    upload_url,
                    file_bytes,
                    media.filename or f"file.{ok_type.lower()}",
                    media.mime_type or "application/octet-stream",
                )

                # Добавляем в список аттачментов
                attachment_payloads.append({
                    "type": ok_type,
                    "payload": {"token": token},
                })

                logger.info("[ok] attachment uploaded: type=%s token=%s", ok_type, token)

            except Exception as e:
                logger.error("[ok] failed to upload attachment %s: %s", media.url, e)
                failed_attachments.append(f"📎 {media.filename}")

        # Формируем сообщение
        message_data: Dict[str, Any] = {}
        if text:
            message_data["text"] = text

        if attachment_payloads:
            message_data["attachments"] = attachment_payloads

        # Отправляем сообщение
        if text or attachment_payloads:
            try:
                data = {
                    "recipient": {"chat_id": recipient_id},
                    "message": message_data,
                }
                await self._ok_call("me/messages", data)
                logger.info(
                    "[ok] SENT: recipient=%s text=%r attachments=%d",
                    recipient_id, (text or "")[:50], len(attachment_payloads),
                )
            except Exception as e:
                logger.exception("[ok] Failed to send message to %s: %s", recipient_id, e)
                if not attachment_payloads:
                    failed_attachments.append(f"💬 Текст: {text[:50] if text else 'пусто'}...")

        return failed_attachments

    @staticmethod
    def parse_ok_media(
        msg: Dict[str, Any]
    ) -> Tuple[Union[TextContent, MediaContent], List[MediaContent]]:
        """
        Парсит сообщение из webhook Одноклассников.
        Формат: message.text + message.attachments[]
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
                title = payload.get("title") or "Video"
                attachments.append(
                    MediaContent(
                        type="media",
                        media_type="video",
                        url=video_url,
                        caption=title,
                        filename=f"ok_{message_id}_{idx}.mp4",
                        mime_type="video/mp4",
                    )
                )

            elif att_type == "SHARE":
                # Ссылка — добавляем в текст
                share_url = payload.get("url")
                if share_url and not text:
                    text = share_url

        if not attachments:
            return TextContent(type="text", text=text), []

        primary: Union[TextContent, MediaContent] = TextContent(type="text", text=text)
        return primary, attachments