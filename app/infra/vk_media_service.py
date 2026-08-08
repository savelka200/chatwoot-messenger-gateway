"""VK Media Service - handles media upload/download operations with VK API."""

import asyncio
import io
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)


class VKMediaService:
    """Service for handling media operations with VK API."""

    def __init__(
        self,
        access_token: str,
        group_id: int,
        api_version: str = "5.199",
    ):
        self._access_token = access_token
        self._group_id = group_id
        self._api_version = api_version
        self._http: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._http is None:
            self._http = httpx.AsyncClient(
                base_url="https://api.vk.ru/method",
                timeout=30.0,
                headers={"User-Agent": "chatwoot-integration/1.0"},
            )
        return self._http

    async def close(self) -> None:
        """Close HTTP client."""
        if self._http:
            try:
                await self._http.aclose()
            except Exception:
                pass
            self._http = None

    async def _vk_call(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make VK API call with error handling."""
        client = await self._get_client()
        full_params = {
            **params,
            "access_token": self._access_token,
            "v": self._api_version,
        }
        resp = await client.post(f"/{method}", data=full_params)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            err = data["error"]
            raise RuntimeError(f"VK API error {err.get('error_code')}: {err.get('error_msg')}")
        return data.get("response", data)

    async def download_file(self, url: str, timeout: float = 60.0) -> bytes:
        """Download file from URL to memory."""
        client = await self._get_client()
        # Follow redirects for Chatwoot URLs which return 302
        resp = await client.get(url, timeout=timeout, follow_redirects=True)
        logger.info("[vk-media] Downloaded file from %s: status=%d size=%d", 
                    url[:80], resp.status_code, len(resp.content))
        resp.raise_for_status()
        return resp.content

    async def get_photo_upload_server(self) -> str:
        """Get upload server URL for photos."""
        result = await self._vk_call("photos.getMessagesUploadServer", {})
        return result["upload_url"]

    async def upload_photo(self, upload_url: str, file_bytes: bytes, filename: str = "photo.jpg", max_retries: int = 3) -> Dict[str, Any]:
        """Upload photo to VK server with retry logic."""
        last_error = None
        for attempt in range(max_retries):
            if attempt > 0:
                logger.warning("[vk-media] Retry attempt %d/%d for photo upload", attempt + 1, max_retries)
                await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
            
            try:
                # VK expects multipart form data with field name "photo"
                # Use tuple format: (filename, file_content, content_type)
                files = {"photo": (filename, file_bytes, "image/jpeg")}
                logger.info("[vk-media] Uploading to URL: %s... (attempt %d)", upload_url[:50], attempt + 1)
                logger.info("[vk-media] File size: %d bytes, filename: %s", len(file_bytes), filename)
                
                # Use separate client for file uploads
                upload_client = httpx.AsyncClient(timeout=60.0, follow_redirects=True)
                try:
                    resp = await upload_client.post(upload_url, files=files)
                finally:
                    await upload_client.aclose()
                    
                logger.info("[vk-media] Upload response status: %d", resp.status_code)
                logger.info("[vk-media] Upload response text (first 500 chars): %s", resp.text[:500])
                
                if resp.status_code == 405:
                    logger.warning("[vk-media] Got 405 error, will retry...")
                    last_error = httpx.HTTPStatusError(f"405 Method Not Allowed", request=resp.request, response=resp)
                    continue
                
                resp.raise_for_status()
                try:
                    result = resp.json()
                    logger.info("[vk-media] Raw upload response JSON: %s", result)
                    
                    # Check for errors in response
                    if isinstance(result, dict) and "error" in result:
                        logger.error("[vk-media] Photo upload returned error: %s - %s", 
                                    result.get("error"), result.get("error_descr"))
                        if attempt < max_retries - 1:
                            continue  # Retry
                        raise RuntimeError(f"VK photo upload error: {result.get('error')} - {result.get('error_descr')}")
                    
                    return result
                except json.JSONDecodeError as e:
                    logger.error("[vk-media] Failed to parse JSON response: %s. Response text: %s", e, resp.text[:500])
                    raise
                    
            except httpx.HTTPStatusError as e:
                last_error = e
                logger.error("[vk-media] HTTP error on attempt %d: %s", attempt + 1, e)
                if attempt < max_retries - 1:
                    continue
                raise
            except Exception as e:
                last_error = e
                logger.error("[vk-media] Unexpected error on attempt %d: %s", attempt + 1, e)
                if attempt < max_retries - 1:
                    continue
                raise
        
        # All retries exhausted
        logger.error("[vk-media] All %d retry attempts failed for photo upload", max_retries)
        if last_error:
            raise last_error
        raise RuntimeError("Photo upload failed after all retries")

    async def save_messages_photo(
        self, photo: str, server: int, hash_: str
    ) -> List[Dict[str, Any]]:
        """Save uploaded photo."""
        result = await self._vk_call(
            "photos.saveMessagesPhoto",
            {"photo": photo, "server": server, "hash": hash_},
        )
        return result

    async def get_doc_upload_server(
        self, peer_id: int, doc_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get upload server URL for documents."""
        params = {"peer_id": peer_id}
        if doc_type:
            params["type"] = doc_type
        logger.info("[vk-media] Calling docs.getMessagesUploadServer with params: %s", params)
        result = await self._vk_call("docs.getMessagesUploadServer", params)
        logger.info("[vk-media] Got upload server response: %s", {k: v for k, v in result.items() if k != 'upload_url'})
        return result

    async def upload_document(
        self, upload_url: str, file_bytes: bytes, filename: str, mime_type: str, max_retries: int = 3
    ) -> Dict[str, Any]:
        """Upload document to VK server with retry logic."""
        client = await self._get_client()
        
        last_error = None
        for attempt in range(max_retries):
            if attempt > 0:
                logger.warning("[vk-media] Retry attempt %d/%d for document upload", attempt + 1, max_retries)
                await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
            
            try:
                # VK expects the field name to be exactly "file" for documents
                logger.info("[vk-media] Uploading document to URL: %s...", upload_url[:200])
                logger.info("[vk-media] File size: %d bytes, filename: %s, mime_type: %s (attempt %d)", 
                            len(file_bytes), filename, mime_type, attempt + 1)
                
                # Создаём BytesIO объект для файла
                file_obj = io.BytesIO(file_bytes)
                
                # Формируем multipart/form-data запрос
                # Имя поля должно быть именно "file" согласно документации VK
                files = {"file": (filename, file_obj, mime_type)}
                
                # Добавляем заголовки, так как VK может их требовать
                headers = {
                    "Origin": "https://api.vk.ru",
                    "Referer": "https://api.vk.ru/",
                    "User-Agent": "VKAndroidApp/5.199-23065 (Android 11; SDK 30; arm64-v8a; ru; 1920x1080)",
                }
                
                # Используем отдельный клиент для загрузки файлов с follow_redirects
                upload_client = httpx.AsyncClient(timeout=60.0, follow_redirects=True)
                try:
                    resp = await upload_client.post(upload_url, files=files, headers=headers)
                finally:
                    await upload_client.aclose()
                
                logger.info("[vk-media] Document upload response status: %d", resp.status_code)
                logger.info("[vk-media] Response headers: %s", dict(resp.headers))
                logger.info("[vk-media] Document upload response text (first 1000 chars): %s", resp.text[:1000])
                
                if resp.status_code == 405:
                    logger.warning("[vk-media] Got 405 error, will retry...")
                    last_error = httpx.HTTPStatusError(f"405 Method Not Allowed", request=resp.request, response=resp)
                    continue
                
                if resp.status_code != 200:
                    logger.error("[vk-media] Upload server returned non-200 status: %d", resp.status_code)
                    # Пробуем распарсить ответ даже при ошибке
                    try:
                        result = resp.json()
                        logger.error("[vk-media] Error response JSON: %s", result)
                    except:
                        pass
                    resp.raise_for_status()
                
                try:
                    result = resp.json()
                    logger.info("[vk-media] Raw document upload response JSON: %s", result)
                    
                    # Проверяем, есть ли ошибка в ответе
                    if isinstance(result, dict) and "error" in result:
                        logger.error("[vk-media] Upload returned error: %s - %s", 
                                    result.get("error"), result.get("error_descr"))
                        if attempt < max_retries - 1:
                            continue  # Пробуем ещё раз
                        raise RuntimeError(f"VK upload error: {result.get('error')} - {result.get('error_descr')}")
                    
                    return result
                except json.JSONDecodeError as e:
                    logger.error("[vk-media] Failed to parse document upload JSON response: %s. Response text: %s", 
                                e, resp.text[:500])
                    raise
                    
            except httpx.HTTPStatusError as e:
                last_error = e
                logger.error("[vk-media] HTTP error on attempt %d: %s", attempt + 1, e)
                if attempt < max_retries - 1:
                    continue
                raise
            except Exception as e:
                last_error = e
                logger.error("[vk-media] Unexpected error on attempt %d: %s", attempt + 1, e)
                if attempt < max_retries - 1:
                    continue
                raise
        
        # Все попытки исчерпаны
        logger.error("[vk-media] All %d retry attempts failed", max_retries)
        if last_error:
            raise last_error
        raise RuntimeError("Document upload failed after all retries")

    async def save_doc(self, file: str, title: str) -> Dict[str, Any]:
        """Save uploaded document."""
        result = await self._vk_call("docs.save", {"file": file, "title": title})
        return result

    async def process_incoming_attachments(
        self, attachments: List[Dict[str, Any]]
    ) -> Tuple[List[bytes], List[str]]:
        """
        Process incoming VK attachments and return file bytes and filenames.
        
        Returns:
            Tuple of (list of file bytes, list of filenames)
        """
        files_bytes = []
        filenames = []
        
        logger.info("[vk-media] Processing %d attachments", len(attachments))
        
        for att in attachments:
            att_type = att.get("type")
            logger.info("[vk-media] Processing attachment type: %s", att_type)
            try:
                if att_type == "photo":
                    photo_obj = att.get("photo", {})
                    logger.info("[vk-media] Photo object: id=%s sizes_count=%d", 
                                photo_obj.get("id"), len(photo_obj.get("sizes", [])))
                    # Get the largest size URL
                    sizes = photo_obj.get("sizes", [])
                    if sizes:
                        # Sort by width to get largest
                        largest = max(sizes, key=lambda s: s.get("width", 0))
                        url = largest.get("url") or photo_obj.get("photo_2521") or photo_obj.get("photo_1280") or photo_obj.get("photo_807") or photo_obj.get("photo_604")
                        if url:
                            logger.info("[vk-media] Downloading photo from: %s", url[:80])
                            file_bytes = await self.download_file(url)
                            files_bytes.append(file_bytes)
                            filenames.append(f"photo_{photo_obj.get('id', 'unknown')}.jpg")
                            logger.info("[vk-media] Downloaded %d bytes as %s", len(file_bytes), filenames[-1])
                        else:
                            logger.warning("[vk-media] No photo URL found in sizes")
                    else:
                        logger.warning("[vk-media] No sizes in photo object")
                
                elif att_type == "doc":
                    doc_obj = att.get("doc", {})
                    url = doc_obj.get("url")
                    logger.info("[vk-media] Document: id=%s url_present=%s", 
                                doc_obj.get("id"), url is not None)
                    if url:
                        file_bytes = await self.download_file(url)
                        files_bytes.append(file_bytes)
                        filename = doc_obj.get("title", f"doc_{doc_obj.get('id', 'unknown')}")
                        filenames.append(filename)
                        logger.info("[vk-media] Downloaded %d bytes as %s", len(file_bytes), filename)
                    else:
                        logger.warning("[vk-media] No URL in document object")
                
                elif att_type == "audio_message":
                    audio_obj = att.get("audio_message", {})
                    url = audio_obj.get("link") or audio_obj.get("url")
                    logger.info("[vk-media] Audio message: id=%s url_present=%s", 
                                audio_obj.get("id"), url is not None)
                    if url:
                        file_bytes = await self.download_file(url, timeout=120.0)
                        files_bytes.append(file_bytes)
                        filenames.append(f"voice_{audio_obj.get('id', 'unknown')}.ogg")
                        logger.info("[vk-media] Downloaded %d bytes as %s", len(file_bytes), filenames[-1])
                    else:
                        logger.warning("[vk-media] No URL in audio_message object")
                
                elif att_type == "video":
                    # Video attachments are tricky - VK usually sends a link, not direct file
                    video_obj = att.get("video", {})
                    logger.info("[vk-media] Video attachment detected, skipping file download: %s", 
                                video_obj.get("id"))
                
                else:
                    logger.warning("[vk-media] Unknown attachment type: %s", att_type)
                    
            except Exception as e:
                logger.exception("[vk-media] Failed to process attachment %s: %s", att_type, e)
        
        logger.info("[vk-media] Successfully processed %d/%d attachments", len(files_bytes), len(attachments))
        return files_bytes, filenames

    def build_attachment_string(self, media_type: str, owner_id: int, media_id: int) -> str:
        """Build VK attachment string format: <type><owner_id>_<media_id>."""
        return f"{media_type}{owner_id}_{media_id}"

    async def upload_and_save_photo(self, file_bytes: bytes, peer_id: int, filename: str = "photo.jpg") -> Optional[str]:
        """
        Upload photo to VK and return attachment string.
        
        Args:
            file_bytes: Photo file content
            peer_id: Recipient peer ID  
            filename: Filename for the upload (should end with .jpg, .png, etc.)
        
        Returns:
            Attachment string like 'photo100_555' or None if failed
        """
        try:
            logger.info("[vk-media] Starting photo upload for peer_id=%d filename=%s", peer_id, filename)
            
            # Step 1: Get upload server
            upload_url = await self.get_photo_upload_server()
            logger.info("[vk-media] Got photo upload server URL: %s...", upload_url[:60])
            
            # Step 2: Upload photo
            logger.info("[vk-media] Uploading %d bytes to VK...", len(file_bytes))
            upload_result = await self.upload_photo(upload_url, file_bytes, filename=filename)
            logger.info("[vk-media] Upload response keys: %s", list(upload_result.keys()) if isinstance(upload_result, dict) else type(upload_result))
            
            # Step 3: Save photo
            photo_str = upload_result.get("photo", "")
            server = upload_result.get("server", 0)
            hash_val = upload_result.get("hash", "")
            logger.info("[vk-media] Photo upload result: photo_len=%d server=%s hash=%s", 
                        len(photo_str) if photo_str else 0, server, hash_val[:20] if hash_val else None)
            
            if not photo_str or not server or not hash_val:
                logger.error("[vk-media] Missing required fields in upload result: photo=%s server=%s hash=%s",
                            bool(photo_str), server, bool(hash_val))
                logger.error("[vk-media] Full upload result: %s", upload_result)
                return None
            
            saved = await self.save_messages_photo(photo_str, server, hash_val)
            logger.info("[vk-media] Save result: %s", saved)
            
            if saved:
                photo_data = saved[0]
                owner_id = photo_data.get("owner_id", -self._group_id)
                photo_id = photo_data.get("id", 0)
                result = self.build_attachment_string("photo", owner_id, photo_id)
                logger.info("[vk-media] Built attachment string: %s", result)
                return result
            else:
                logger.error("[vk-media] save_messages_photo returned empty result")
        except Exception as e:
            logger.exception("[vk-media] Failed to upload photo: %s", e)
        return None

    async def upload_and_save_document(
        self, 
        file_bytes: bytes, 
        peer_id: int, 
        filename: str,
        mime_type: str = "application/octet-stream",
        doc_type: Optional[str] = None
    ) -> Optional[str]:
        """
        Upload document to VK and return attachment string.
        
        Args:
            file_bytes: File content
            peer_id: Recipient peer ID
            filename: Original filename
            mime_type: MIME type of the file
            doc_type: Type of document (e.g., "audio_message" for voice)
        
        Returns:
            Attachment string like 'doc100_555' or None if failed
        """
        try:
            logger.info("[vk-media] Starting document upload: filename=%s mime_type=%s doc_type=%s",
                        filename, mime_type, doc_type)
            
            # Step 1: Get upload server
            server_info = await self.get_doc_upload_server(peer_id, doc_type)
            upload_url = server_info.get("upload_url", "")
            logger.info("[vk-media] Got document upload server URL")
            
            if not upload_url:
                logger.error("[vk-media] Empty upload URL from server")
                return None
            
            # Step 2: Upload document
            logger.info("[vk-media] Uploading %d bytes to VK...", len(file_bytes))
            upload_result = await self.upload_document(upload_url, file_bytes, filename, mime_type)
            logger.info("[vk-media] Upload response keys: %s", upload_result.keys() if isinstance(upload_result, dict) else type(upload_result))
            
            # Step 3: Save document
            file_str = upload_result.get("file", "")
            logger.info("[vk-media] Document upload result: file_len=%d", len(file_str) if file_str else 0)
            
            if not file_str:
                logger.error("[vk-media] Missing 'file' field in upload result")
                return None
            
            saved = await self.save_doc(file_str, filename)
            logger.info("[vk-media] Save result: %s", saved)
            
            if saved:
                # Handle both 'doc' and 'audio_message' response formats
                if 'doc' in saved:
                    doc_data = saved['doc']
                elif 'audio_message' in saved:
                    doc_data = saved['audio_message']
                else:
                    doc_data = saved
                
                # Get owner_id and doc_id from the response
                # owner_id can be negative for group docs, positive for user docs
                owner_id = doc_data.get("owner_id", 0)
                doc_id = doc_data.get("id", 0)
                
                logger.info("[vk-media] Extracted owner_id=%d, doc_id=%d from save response", owner_id, doc_id)
                
                if doc_id == 0:
                    logger.error("[vk-media] Document ID is 0, something went wrong in docs.save")
                    return None
                
                result = self.build_attachment_string("doc", owner_id, doc_id)
                logger.info("[vk-media] Built attachment string: %s", result)
                return result
            else:
                logger.error("[vk-media] docs.save returned empty result")
        except Exception as e:
            logger.exception("[vk-media] Failed to upload document: %s", e)
        return None

    async def upload_and_save_audio_message(
        self, 
        file_bytes: bytes, 
        peer_id: int,
        filename: str = "voice.ogg"
    ) -> Optional[str]:
        """
        Upload voice message to VK.
        Voice messages must be in .ogg format.
        
        Returns:
            Attachment string like 'doc100_555' or None if failed
        """
        return await self.upload_and_save_document(
            file_bytes=file_bytes,
            peer_id=peer_id,
            filename=filename,
            mime_type="audio/ogg",
            doc_type="audio_message"
        )
