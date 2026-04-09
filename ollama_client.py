import os
import requests
from dotenv import load_dotenv

load_dotenv()

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma4:latest")


def build_manager_prompt(summary):
    return f"""
Sen bir üretim fabrikasında sabah toplantısı yapan kıdemli bir yöneticisin.

Aşağıdaki veriler önceden analiz edildi.
Kurallar zaten uygulanmış durumda.
Senin görevin sadece yönetici diliyle kısa ve net yorum yapmak.

Kurallar:
- Hesaplama yapma
- Verilen yorumları bozma
- En kritik 3 noktayı öne çıkar
- Türkçe yaz
- Kısa ve profesyonel ol

Veri:
{summary}
"""


def ask_ollama(prompt):
    response = requests.post(
        f"{OLLAMA_BASE_URL}/api/generate",
        json={
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False
        },
        timeout=120
    )
    response.raise_for_status()
    data = response.json()
    return data.get("response", "").strip()