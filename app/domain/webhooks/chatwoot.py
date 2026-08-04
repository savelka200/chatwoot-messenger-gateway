from typing import List, Optional

from pydantic import BaseModel


class ChatwootAttachment(BaseModel):
    """Chatwoot message attachment."""
    id: Optional[int] = None
    data_url: Optional[str] = None  # Full URL to download
    file_url: Optional[str] = None   # Public URL
    thumb_url: Optional[str] = None
    filename: Optional[str] = None
    content_type: Optional[str] = None


class ChatwootConversationMeta(BaseModel):
    channel: Optional[str] = None  # "whatsapp" | "telegram" | "vk"
    recipient_id: Optional[str] = None  # unified recipient id


class ChatwootConversation(BaseModel):
    meta: ChatwootConversationMeta = ChatwootConversationMeta()


class ChatwootMessageCreatedWebhook(BaseModel):
    """Minimal model for Chatwoot event=message_created."""

    event: str
    message_type: Optional[str] = None  # "incoming" | "outgoing"
    private: Optional[bool] = None
    content: Optional[str] = None
    conversation: ChatwootConversation = ChatwootConversation()
    attachments: Optional[List[ChatwootAttachment]] = None
