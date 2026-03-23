from __future__ import annotations

from app.services.openai_client import get_openai_client


def generate_conversation_reply(user_prompt: str) -> str:
    client = get_openai_client()
    response = client.responses.create(
        model="gpt-5",
        input=[
            {
                "role": "system",
                "content": (
                    "You are a friendly analytics assistant in a chat UI. "
                    "For casual conversation, reply naturally in 1-2 short sentences. "
                    "Do not generate SQL unless explicitly asked for data analysis."
                ),
            },
            {"role": "user", "content": user_prompt},
        ],
    )
    return (response.output_text or "").strip() or "How can I help you today?"

