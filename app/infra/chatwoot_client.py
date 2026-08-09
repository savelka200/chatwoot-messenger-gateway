import logging
from typing import Any, Dict, List, Optional, Tuple

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
        **extra_fields: Any,
    ) -> Dict[str, Any]:
        """Send a message to a conversation."""
        url = f"{self._account_base}/conversations/{conversation_id}/messages"
        payload: Dict[str, Any] = {"content": content}
        if extra_fields:
            payload.update(extra_fields)

        async with httpx.AsyncClient(headers=self._headers, timeout=15.0) as client:
            r = await client.post(url, json=payload)
            r.raise_for_status()
            return r.json()

    async def send_message_with_attachments(
        self,
        conversation_id: int,
        content: Optional[str],
        files: List[Tuple[str, bytes, str]],
        **extra_fields: Any,
    ) -> Dict[str, Any]:
        """
        Send a message with file attachments to a conversation.
        
        Args:
            conversation_id: Chatwoot conversation ID
            content: Message text (can be None if only attachments)
            files: List of tuples (filename, file_bytes, mime_type)
            extra_fields: Additional fields like message_type
        
        Returns:
            Response data from Chatwoot API
        """
        url = f"{self._account_base}/conversations/{conversation_id}/messages"
        
        # Build multipart form data using httpx.Files type
        # httpx expects files as a dict or list of tuples
        form_data: Dict[str, Any] = {}
        # Only include content if it's not empty - Chatwoot requires non-empty content
        # for messages with attachments, so we use a placeholder for attachment-only messages
        if content:
            form_data["content"] = content
        elif files:
            # For attachment-only messages, use empty string but ensure attachments are sent
            # Chatwoot API accepts empty content when attachments are present
            form_data["content"] = ""
        
        # Add message_type if provided
        message_type = extra_fields.get("message_type")
        if message_type:
            form_data["message_type"] = message_type
        
        # Build files list for httpx - multiple files with same field name require list of tuples
        import io
        files_list: List[Tuple[str, Any]] = []
        for idx, (filename, file_bytes, mime_type) in enumerate(files):
            logger.info("[chatwoot-client] Adding attachment %d: %s (%d bytes, %s)", 
                       idx, filename, len(file_bytes), mime_type)
            logger.info("[chatwoot-client] Attachment %d first 100 bytes: %r", idx, file_bytes[:100])
            # Use BytesIO wrapper for better compatibility
            file_obj = io.BytesIO(file_bytes)
            # For multiple files with same field name, httpx expects list of tuples
            files_list.append((f"attachments[]", (filename, file_obj, mime_type)))
        
        # Custom headers without Content-Type (httpx will set it with boundary)
        headers = {
            "api_access_token": self._headers["api_access_token"],
            "Authorization": self._headers["Authorization"],
        }
        
        logger.info("[chatwoot-client] Sending multipart message to %s with %d files", url, len(files))
        logger.info("[chatwoot-client] Content field: %r", content)
        logger.info("[chatwoot-client] Form data fields: %s", list(form_data.keys()))
        logger.info("[chatwoot-client] Files to upload: %d", len(files_list))
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(url, headers=headers, data=form_data, files=files_list)
            logger.info("[chatwoot-client] Response status: %d", r.status_code)
            logger.info("[chatwoot-client] Response headers: %s", dict(r.headers))
            logger.info("[chatwoot-client] Response body (first 1000 chars): %s", r.text[:1000])
            try:
                response_json = r.json()
                logger.info("[chatwoot-client] Response JSON: %s", response_json)
            except Exception as json_err:
                logger.warning("[chatwoot-client] Failed to parse JSON response: %s", json_err)
            r.raise_for_status()
            return r.json()
