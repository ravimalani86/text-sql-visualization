from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from app.db.engine import engine
from app.repositories.history_repo import get_conversation_with_turns, list_conversations


router = APIRouter(tags=["history"])


@router.get("/history/conversations")
async def history_conversations() -> Dict[str, Any]:
    return {"items": list_conversations(engine)}


@router.get("/history/{conversation_id}")
async def history_conversation(conversation_id: str) -> Dict[str, Any]:
    payload = get_conversation_with_turns(engine, conversation_id)
    if not payload:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return payload

