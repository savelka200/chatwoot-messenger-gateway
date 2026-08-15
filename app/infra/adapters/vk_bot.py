import logging
import secrets
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, Union, List
import mimetypes
import httpx  # NEW
from pyee.asyncio import AsyncIOEventEmitter

from app.config import VKCommunityConfig
from app.domain.message import TextContent, UnifiedMessage, MediaContent
from app.domain.ports import MessengerAdapter, OnMessage

logger = logging.getLogger(__name__)


class VkAdapter(MessengerAdapter):
    """VK adapter for Callback API (text only)."""

    def __init__(self, bus: AsyncIOEventEmitter, config: VKCommunityConfig):
        self._bus = bus
        self._config = config
        self.inbox_id = config.inbox_id
        self._cb: Optional[OnMessage] = None
        self._incoming_listener: Optional[Callable[..., Awaitable[None]]] = None
        self._confirm_listener: Optional[Callable[..., Awaitable[None]]] = None
        self._http: Optional[httpx.AsyncClient] = None  # NEW


    @staticmethod
    def format_reply_quote(reply_msg: Dict[str, Any]) -> str:
        """
        Формирует текстовую цитату из reply_message.
        Возвращает строку вида:
          ↩️ Ответ на сообщение:
          "Текст оригинала"
          📎 2 документа
        """
        if not reply_msg:
            return ""

        text = (reply_msg.get("text") or "").strip()
        attachments = reply_msg.get("attachments") or []

        # Подсчитываем типы вложений
        attachment_counts: Dict[str, int] = {}
        for att in attachments:
            att_type = att.get("type", "unknown")
            attachment_counts[att_type] = attachment_counts.get(att_type, 0) + 1

        # Формируем описание вложений
        attachment_descriptions = []
        type_names = {
            "photo": "фото",
            "video": "видео",
            "doc": "документ",
            "audio_message": "голосовое",
            "audio": "аудио",
        }
        for att_type, count in attachment_counts.items():
            name = type_names.get(att_type, att_type)
            # Склонение для русского языка
            if count == 1:
                attachment_descriptions.append(f"📎 {count} {name}")
            elif 2 <= count <= 4:
                attachment_descriptions.append(f"📎 {count} {name}а")
            else:
                attachment_descriptions.append(f"📎 {count} {name}ов")

        # Собираем цитату
        quote_lines = ["↩️ Ответ на сообщение:"]
        if text:
            quote_lines.append(f'"{text}"')
        if attachment_descriptions:
            quote_lines.extend(attachment_descriptions)

        return "\n".join(quote_lines)
    @staticmethod
    def extract_vk_photo_url(photo: Dict[str, Any]) -> Optional[str]:
        #orig_photo → самый крупный size
        orig_url = (photo.get("orig_photo") or {}).get("url")
        if orig_url:
            return orig_url
        sizes = photo.get("sizes") or []
        if not sizes:
            return None
        best = max(sizes, key=lambda s: s.get("width", 0) * s.get("height", 0))
        return best.get("url")


    @classmethod
    def parse_vk_media(
        cls, msg: Dict[str, Any]
    ) -> Tuple[Union[TextContent, MediaContent], List[MediaContent]]:
        """
        Разбирает message из Callback API.
        Поддерживает: фото, видео (превью), документы, голосовые сообщения.
        """
        text = (msg.get("text") or "").strip()
        message_id = str(msg.get("id")) if msg.get("id") is not None else "msg" 

        attachments: List[MediaContent] = []
        for idx, att in enumerate(msg.get("attachments") or []):
            att_type = att.get("type")  

            if att_type == "photo":
                photo = att.get("photo") or {}
                url = cls.extract_vk_photo_url(photo)
                if not url:
                    continue
                caption = (photo.get("text") or "").strip() or None
                attachments.append(
                    MediaContent(
                        type="media",
                        media_type="image",
                        url=url,
                        caption=caption,
                        filename=f"vk_{message_id}_{idx}.jpg",
                        mime_type="image/jpeg",
                    )
                )   

            elif att_type == "video":
                video = att.get("video") or {}
                previews = video.get("image") or []
                if not previews:
                    continue
                best_preview = max(
                    previews,
                    key=lambda p: p.get("width", 0) * p.get("height", 0),
                )
                preview_url = best_preview.get("url")
                if not preview_url:
                    continue
                title = (video.get("title") or "").strip() or None
                attachments.append(
                    MediaContent(
                        type="media",
                        media_type="video",
                        url=preview_url,
                        caption=title,
                        filename=f"vk_{message_id}_{idx}_video_preview.jpg",
                        mime_type="image/jpeg",
                    )
                )   

            elif att_type == "doc":
                doc = att.get("doc") or {}
                doc_url = doc.get("url")
                if not doc_url:
                    continue
                title = (doc.get("title") or "").strip()
                ext = (doc.get("ext") or "").strip()
                if not title and ext:
                    title = f"vk_document.{ext}"
                if not title:
                    title = "vk_document.bin"
                if ext and not title.lower().endswith(f".{ext.lower()}"):
                    title = f"{title}.{ext}"
                guessed, _ = mimetypes.guess_type(title)
                mime_type = guessed or "application/octet-stream"
                size = doc.get("size")
                attachments.append(
                    MediaContent(
                        type="media",
                        media_type="document",
                        url=doc_url,
                        caption=title,
                        filename=title,
                        mime_type=mime_type,
                        raw={"size": size} if size else {},
                    )
                )   

            elif att_type == "audio_message":
                audio_msg = att.get("audio_message") or {}
                # Используем MP3 как более универсальный формат
                audio_url = audio_msg.get("link_mp3") or audio_msg.get("link_ogg")
                if not audio_url:
                    continue
                duration = audio_msg.get("duration") or 0
                # Формируем имя файла с указанием длительности
                filename = f"vk_{message_id}_{idx}_voice_{duration}s.mp3"
                attachments.append(
                    MediaContent(
                        type="media",
                        media_type="audio",
                        url=audio_url,
                        caption=f"Голосовое сообщение ({duration} сек)",
                        filename=filename,
                        mime_type="audio/mpeg" if audio_url.endswith(".mp3") else "audio/ogg",
                        raw={"duration": duration},
                    )
                )   

        if not attachments:
            return TextContent(type="text", text=text), []  

        primary: Union[TextContent, MediaContent] = TextContent(type="text", text=text)
        return primary, attachments
    @classmethod
    def _build_content(cls, msg, *_args):
        return cls.parse_vk_media(msg)

    def on_message(self, cb: OnMessage) -> None:
        self._cb = cb

    def confirmation_token(self) -> str:
        return self._config.confirmation

    async def start(self) -> None:
        # Initialize HTTP client once
        if self._http is None:
            self._http = httpx.AsyncClient(
                base_url="https://api.vk.ru/method",
                timeout=15,
                headers={"User-Agent": "chatwoot-integration/1.0"},
            )

        async def _on_vk_incoming(payload: Dict[str, Any]) -> None:
            if payload.get("event") != "message_new":
                return
        
            msg = payload.get("message") or {}
            peer_id = msg.get("peer_id")
            from_id = msg.get("from_id") or peer_id
            message_id = msg.get("id")
            conversation_message_id = msg.get("conversation_message_id")
        
            if not peer_id:
                logger.debug("[vk] skip incoming: missing peer_id")
                return
        
            # === ПРОВЕРКА НА is_cropped ===
            # ВК присылает is_cropped=true, если в webhook влезли не все вложения
            is_cropped = bool(msg.get("is_cropped"))
            if is_cropped and conversation_message_id is not None:
                logger.info(
                    "[vk] is_cropped=true detected, fetching full message (peer=%s, cmid=%s)",
                    peer_id, conversation_message_id,
                )
                full_msg = await self._fetch_full_message(
                    peer_id=int(peer_id),
                    conversation_message_id=int(conversation_message_id),
                )
                if full_msg:
                    msg = full_msg  # подменяем на полную версию
                    logger.info(
                        "[vk] fetched full message: %d attachments",
                        len(msg.get("attachments") or []),
                    )
                else:
                    logger.warning("[vk] fallback to cropped payload")
        
            # Обновляем ID из (возможно, полного) msg
            from_id = str(msg.get("from_id") or peer_id)
            peer_id_str = str(peer_id)
            message_id = str(msg.get("id")) if msg.get("id") is not None else None
        
            content, attachments = self._build_content(msg)

            reply_msg = msg.get("reply_message")
            if reply_msg:
                reply_quote = self.format_reply_quote(reply_msg)
                logger.info("[vk] reply detected, adding quote to message")

                # Добавляем цитату к основному тексту
                if isinstance(content, TextContent):
                    # Если основной контент — текст, добавляем цитату в начало
                    original_text = content.text.strip()
                    new_text = f"{reply_quote}\n\n{original_text}" if original_text else reply_quote
                    content = TextContent(type="text", text=new_text)
                elif isinstance(content, MediaContent):
                    # Если основной контент — медиа (одно фото без текста),
                    # превращаем его в TextContent с цитатой, а медиа уйдёт в attachments
                    attachments.insert(0, content)  # медиа становится первым вложением
                    content = TextContent(type="text", text=reply_quote)

            umsg = UnifiedMessage(
                channel="vk",
                sender_id=from_id,
                recipient_id=peer_id_str,
                message_id=message_id,
                content=content,
                attachments=attachments,
                raw=payload,  # оригинал сохраняем для отладки
            )
            self._bus.emit("vk.message", umsg)

        async def _on_vk_confirmation(payload: Dict[str, Any]) -> None:
            group_id = payload.get("group_id")
            logger.info("[vk] confirmation request received for group_id=%s", group_id) 
        self._incoming_listener = _on_vk_incoming
        self._confirm_listener = _on_vk_confirmation
        self._bus.on("vk.incoming", self._incoming_listener)
        self._bus.on("vk.confirmation", self._confirm_listener) 
        logger.info("[vk] adapter started (callback API, text only)")

    async def stop(self) -> None:
        if self._incoming_listener:
            try:
                self._bus.remove_listener("vk.incoming", self._incoming_listener)  # type: ignore[attr-defined]
            except Exception:
                pass
            self._incoming_listener = None

        if self._confirm_listener:
            try:
                self._bus.remove_listener("vk.confirmation", self._confirm_listener)  # type: ignore[attr-defined]
            except Exception:
                pass
            self._confirm_listener = None

        # Close HTTP client
        if self._http:
            try:
                await self._http.aclose()
            except Exception:
                pass
            self._http = None

        logger.info("[vk] adapter stopped")

    async def _fetch_full_message(
        self, peer_id: int, conversation_message_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Если ВК прислал is_cropped=true, получаем полное сообщение через API,
        чтобы достать все вложения.
        """
        if not self._http:
            return None
        try:
            data = await self._vk_call(
                "messages.getByConversationMessageId",
                {
                    "peer_id": peer_id,
                    "conversation_message_ids": conversation_message_id,
                },
            )
            # Ответ: {"count": N, "items": [...]}
            items = (data or {}).get("items") or []
            if not items:
                logger.warning("[vk] getByConversationMessageId returned no items")
                return None
            return items[0]
        except Exception as e:
            logger.warning(
                "[vk] failed to fetch full message (peer=%s, cmid=%s): %s",
                peer_id, conversation_message_id, e,
            )
            return None

    async def _vk_call(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """VK API call with basic error handling."""
        if not self._http:
            raise RuntimeError("VK HTTP client is not initialized")

        # Required parameters
        params = {
            **params,
            "access_token": self._config.access_token,
            "v": self._config.api_version,
        }

        resp = await self._http.post(f"/{method}", data=params)
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            err = data["error"]
            code = err.get("error_code")
            msg = err.get("error_msg")
            logger.error("[vk] API error %s: %s; params=%s", code, msg, params)
            raise RuntimeError(f"VK API error {code}: {msg}")

        return data.get("response", data)

    async def send_text(self, recipient_id: str, content: TextContent) -> None:
        """Send a text message via VK messages.send."""
        # recipient_id must be peer_id: user_id, chat peer (2e9+chat_id) or group peer
        text = content.text or ""
        if not text:
            logger.info("[vk] skip send: empty text")
            return

        try:
            random_id = secrets.randbits(31)  # unique random_id per request
            params = {
                "peer_id": int(recipient_id),
                "message": text,
                "random_id": random_id,
                # For a community token, group_id can be omitted, but doesn't hurt
                "group_id": self._config.group_id,
                # Optionally, disable_mentions=1 can be added if needed
            }
            res = await self._vk_call("messages.send", params)
            # Success: VK returns message ID or an array
            logger.info("[vk] SENT: peer_id=%s message_id=%s", recipient_id, res)
        except Exception as e:
            logger.exception("[vk] Failed to send text to %s: %s", recipient_id, e)
