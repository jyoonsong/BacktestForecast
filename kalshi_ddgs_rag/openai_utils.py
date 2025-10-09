from .config import client, MODEL_NAME
from .utils import log

def run_openai(prompt: str, model: str = MODEL_NAME) -> str:
    """Run an OpenAI chat completion."""
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        log(f"OpenAI API error: {e}")
        return ""
