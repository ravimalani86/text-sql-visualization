import os

from openai import OpenAI


def _client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY environment variable is not set")
    return OpenAI(api_key=api_key)


def generate_conversation_reply(user_prompt: str) -> str:
    response = _client().responses.create(
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
