from typing import Any, Dict, List, Optional, Tuple
import logging
import httpx

logger = logging.getLogger(__name__)

class ChatwootClient:
    """
    Lightweight HTTP client for Chatwoot API v1.
    Only methods needed by our service are implemented.
    """

    def __init__(self, api_access_token: str, account_id: int, base_url: str):
        # Normalize base_url and store common parts
        self._base_url = base_url.rstrip("/")
        self._account_id = account_id

        # Precomputed base for account-scoped endpoints
        self._account_base = f"{self._base_url}/api/v1/accounts/{self._account_id}"

        # Static headers with API token (add both headers for compatibility)
        self._headers = {
            "Content-Type": "application/json",
            "api_access_token": api_access_token,
            "Authorization": f"Bearer {api_access_token}",
        }

    # Contacts
    async def search_contacts(self, q: str) -> Dict[str, Any]:
        """Search contacts by name/identifier/email/phone."""
        url = f"{self._account_base}/contacts/search"
        params = {"q": q}
        async with httpx.AsyncClient(headers=self._headers, timeout=15.0) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            return r.json()

    async def filter_contacts(self, attrs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter contacts by attributes supported by /contacts/filter.
        attribute_key MUST be the raw key (e.g., "vk_user_id"), not "custom_attribute_*".
        """
        url = f"{self._account_base}/contacts/filter"
        filters: List[Dict[str, Any]] = []
        for key, value in attrs.items():
            filters.append(
                {
                    "attribute_key": key,
                    "filter_operator": "equal_to",
                    "values": [str(value)],
                }
            )
        payload = {"payload": filters}

        async with httpx.AsyncClient(headers=self._headers, timeout=15.0) as client:
            r = await client.post(url, json=payload)
            r.raise_for_status()
            return r.json()

    async def create_contact(
        self,
        *,
        inbox_id: int,
        name: Optional[str] = None,
        phone_number: Optional[str] = None,
        email: Optional[str] = None,
        identifier: Optional[str] = None,
        avatar_url: Optional[str],
        custom_attributes: Optional[Dict[str, Any]] = None,
        additional_attributes: Optional[Dict[str, Any]] = None,  # NEW
    ) -> Dict[str, Any]:
        """Create a contact in a specific inbox (inbox_id is required by API)."""
        url = f"{self._account_base}/contacts"
        payload: Dict[str, Any] = {"inbox_id": inbox_id}

        if name:
            payload["name"] = name
        if phone_number:
            payload["phone_number"] = (
                phone_number if phone_number.startswith("+") else f"+{phone_number}"
            )
        if email:
            payload["email"] = email
        if identifier:
            payload["identifier"] = identifier
        if custom_attributes:
            payload["custom_attributes"] = custom_attributes
        if avatar_url:
            payload["avatar_url"] = avatar_url
        if additional_attributes:
            payload["additional_attributes"] = additional_attributes
            
        async with httpx.AsyncClient(headers=self._headers, timeout=15.0) as client:
            r = await client.post(url, json=payload)
            r.raise_for_status()
            return r.json()

    async def update_contact(
        self,
        *,
        contact_id: int,
        name: Optional[str] = None,
        phone_number: Optional[str] = None,
        email: Optional[str] = None,
        identifier: Optional[str] = None,
        avatar_url: Optional[str],
        custom_attributes: Optional[Dict[str, Any]] = None,
        additional_attributes: Optional[Dict[str, Any]] = None,  # NEW
    ) -> Dict[str, Any]:
        """Patch contact fields, custom attributes, and additional attributes."""
        url = f"{self._account_base}/contacts/{contact_id}"
        payload: Dict[str, Any] = {}
        if name is not None:
            payload["name"] = name
        if phone_number is not None:
            payload["phone_number"] = (
                phone_number if phone_number.startswith("+") else f"+{phone_number}"
            )
        if email is not None:
            payload["email"] = email
        if identifier:
            payload["identifier"] = identifier
        if custom_attributes is not None:
            payload["custom_attributes"] = custom_attributes
        if additional_attributes is not None:
            payload["additional_attributes"] = additional_attributes
        if avatar_url is not None:
            payload["avatar_url"] = avatar_url

        async with httpx.AsyncClient(headers=self._headers, timeout=15.0) as client:
            r = await client.patch(url, json=payload)
            r.raise_for_status()
            return r.json()

    # Conversations
    async def list_conversations(self, contact_id: int) -> Dict[str, Any]:
        """List conversations for a contact."""
        url = f"{self._account_base}/contacts/{contact_id}/conversations"
        async with httpx.AsyncClient(headers=self._headers, timeout=15.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            return r.json()

    async def create_conversation(
        self,
        *,
        inbox_id: int,
        source_id: str,
        contact_id: Optional[int] = None,
        **extra_fields: Any,
    ) -> Dict[str, Any]:
        """Create a conversation bound to source_id in the given inbox (API requires inbox_id)."""
        url = f"{self._account_base}/conversations"
        payload: Dict[str, Any] = {"source_id": source_id, "inbox_id": inbox_id}
        if contact_id:
            payload["contact_id"] = contact_id
        if extra_fields:
            payload.update(extra_fields)

        async with httpx.AsyncClient(headers=self._headers, timeout=15.0) as client:
            r = await client.post(url, json=payload)
            r.raise_for_status()
            return r.json()

    # Messages

    async def send_message(
        self,
        conversation_id: int,
        content: str,
        attachments: Optional[List[Tuple[str, bytes, str]]] = None,
        **extra_fields: Any,
    ) -> Dict[str, Any]:
        """
        Send a message to a conversation.
        - Text only -> application/json
        - With attachments -> multipart/form-data
        attachments: list of (filename, file_bytes, mime_type)
        """
        import io

        url = f"{self._account_base}/conversations/{conversation_id}/messages"

    async def send_message(
        self,
        conversation_id: int,
        content: str,
        attachments: Optional[List[Tuple[str, bytes, str]]] = None,
        **extra_fields: Any,
    ) -> Dict[str, Any]:
        import io
    
        url = f"{self._account_base}/conversations/{conversation_id}/messages"
    
        # Только auth-заголовки, БЕЗ Content-Type
        auth_headers = {
            "api_access_token": self._headers.get("api_access_token"),
            "Authorization": self._headers.get("Authorization"),
        }
    
        if attachments:
            # Определяем file_type
            first_mime = attachments[0][2] if attachments else "image/jpeg"
            if "image" in first_mime:
                file_type = "image"
            elif "video" in first_mime:
                file_type = "video"
            elif "audio" in first_mime:
                file_type = "audio"
            else:
                file_type = "file"
    
            # Всё через files=[] в формате (field, (filename_or_None, content, mime))
            # Текстовые поля: filename=None означает обычное form-field
            files = [
                ("attachments[]", (filename, io.BytesIO(body), mime_type))
                for filename, body, mime_type in attachments
            ]
            files.append(("content", (None, content or "", None)))
            files.append(("message_type", (None, str(extra_fields.get("message_type", "outgoing")), None)))
            files.append(("file_type", (None, file_type, None)))
    
            logger.info("[chatwoot] Sending multipart: %d files, file_type=%s",
                       len(attachments), file_type)
    
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.post(url, files=files, headers=auth_headers)
        else:
            # Для JSON используем стандартные заголовки
            headers = {**self._headers}  # копия со всеми полями
            payload: Dict[str, Any] = {"content": content}
            payload.update(extra_fields)
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.post(url, json=payload, headers=headers)
    
        if r.status_code >= 400:
            logger.error(
                "[chatwoot] send_message failed: status=%s response=%s",
                r.status_code,
                r.text
            )
        r.raise_for_status()
        return r.json()
