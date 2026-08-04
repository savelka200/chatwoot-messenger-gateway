from typing import Any, Dict, List, Optional

import httpx


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

    async def get_message(
        self,
        conversation_id: int,
        message_id: int,
    ) -> Dict[str, Any]:
        """Get a specific message from a conversation (to fetch attachments)."""
        url = f"{self._account_base}/conversations/{conversation_id}/messages/{message_id}"
        async with httpx.AsyncClient(headers=self._headers, timeout=15.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            return r.json()
