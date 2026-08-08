"""VK Media Service - handles media upload/download operations with VK API."""

import io
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
        resp = await client.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.content

    async def get_photo_upload_server(self) -> str:
        """Get upload server URL for photos."""
        result = await self._vk_call("photos.getMessagesUploadServer", {})
        return result["upload_url"]

    async def upload_photo(self, upload_url: str, file_bytes: bytes) -> Dict[str, Any]:
        """Upload photo to VK server."""
        client = await self._get_client()
        # Use multipart form data for upload
        files = {"photo": ("photo.jpg", file_bytes, "image/jpeg")}
        resp = await client.post(upload_url, files=files)
        resp.raise_for_status()
        return resp.json()

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
        result = await self._vk_call("docs.getMessagesUploadServer", params)
        return result

    async def upload_document(
        self, upload_url: str, file_bytes: bytes, filename: str, mime_type: str
    ) -> Dict[str, Any]:
        """Upload document to VK server."""
        client = await self._get_client()
        files = {"file": (filename, file_bytes, mime_type)}
        resp = await client.post(upload_url, files=files)
        resp.raise_for_status()
        return resp.json()

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
        
        for att in attachments:
            att_type = att.get("type")
            try:
                if att_type == "photo":
                    photo_obj = att.get("photo", {})
                    # Get the largest size URL
                    sizes = photo_obj.get("sizes", [])
                    if sizes:
                        # Sort by width to get largest
                        largest = max(sizes, key=lambda s: s.get("width", 0))
                        url = largest.get("url") or photo_obj.get("photo_2521") or photo_obj.get("photo_1280") or photo_obj.get("photo_807") or photo_obj.get("photo_604")
                        if url:
                            file_bytes = await self.download_file(url)
                            files_bytes.append(file_bytes)
                            filenames.append(f"photo_{photo_obj.get('id', 'unknown')}.jpg")
                
                elif att_type == "doc":
                    doc_obj = att.get("doc", {})
                    url = doc_obj.get("url")
                    if url:
                        file_bytes = await self.download_file(url)
                        files_bytes.append(file_bytes)
                        filename = doc_obj.get("title", f"doc_{doc_obj.get('id', 'unknown')}")
                        filenames.append(filename)
                
                elif att_type == "audio_message":
                    audio_obj = att.get("audio_message", {})
                    url = audio_obj.get("link") or audio_obj.get("url")
                    if url:
                        file_bytes = await self.download_file(url, timeout=120.0)
                        files_bytes.append(file_bytes)
                        filenames.append(f"voice_{audio_obj.get('id', 'unknown')}.ogg")
                
                elif att_type == "video":
                    # Video attachments are tricky - VK usually sends a link, not direct file
                    video_obj = att.get("video", {})
                    logger.info("[vk-media] Video attachment detected, skipping file download: %s", video_obj.get("id"))
                
                else:
                    logger.warning("[vk-media] Unknown attachment type: %s", att_type)
                    
            except Exception as e:
                logger.exception("[vk-media] Failed to process attachment %s: %s", att_type, e)
        
        return files_bytes, filenames

    def build_attachment_string(self, media_type: str, owner_id: int, media_id: int) -> str:
        """Build VK attachment string format: <type><owner_id>_<media_id>."""
        return f"{media_type}{owner_id}_{media_id}"

    async def upload_and_save_photo(self, file_bytes: bytes, peer_id: int) -> Optional[str]:
        """
        Upload photo to VK and return attachment string.
        
        Returns:
            Attachment string like 'photo100_555' or None if failed
        """
        try:
            # Step 1: Get upload server
            upload_url = await self.get_photo_upload_server()
            
            # Step 2: Upload photo
            upload_result = await self.upload_photo(upload_url, file_bytes)
            
            # Step 3: Save photo
            photo_str = upload_result.get("photo", "")
            server = upload_result.get("server", 0)
            hash_val = upload_result.get("hash", "")
            
            saved = await self.save_messages_photo(photo_str, server, hash_val)
            if saved:
                photo_data = saved[0]
                owner_id = photo_data.get("owner_id", -self._group_id)
                photo_id = photo_data.get("id", 0)
                return self.build_attachment_string("photo", owner_id, photo_id)
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
            # Step 1: Get upload server
            server_info = await self.get_doc_upload_server(peer_id, doc_type)
            upload_url = server_info.get("upload_url", "")
            
            # Step 2: Upload document
            upload_result = await self.upload_document(upload_url, file_bytes, filename, mime_type)
            
            # Step 3: Save document
            file_str = upload_result.get("file", "")
            saved = await self.save_doc(file_str, filename)
            if saved:
                doc_data = saved
                owner_id = doc_data.get("owner_id", -self._group_id)
                doc_id = doc_data.get("id", 0)
                return self.build_attachment_string("doc", owner_id, doc_id)
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
