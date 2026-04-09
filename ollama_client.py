import os
import requests
from dotenv import load_dotenv

load_dotenv()

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL")


FORBIDDEN_AI_TERMS = [
    "satis",
    "pazarlama",
    "finans",
    "stddev",
    "mean",
    "varyans",
    "varsayimsal",
    "gelir",
]


def _normalize_text(text):
    if not text:
        return ""

    replacements = str.maketrans(
        {
            "ç": "c",
            "ğ": "g",
            "ı": "i",
            "İ": "i",
            "ö": "o",
            "ş": "s",
            "ü": "u",
            "Ç": "c",
            "Ğ": "g",
            "Ö": "o",
            "Ş": "s",
            "Ü": "u",
        }
    )
    return str(text).translate(replacements).lower()


def _format_action_line(item):
    comparison = item.get("karsilastirma")
    relation = item.get("relation_label")
    if comparison:
        return f"- [{item['kategori']}] {item['parametre']}: {comparison} ({relation})"
    return f"- [{item['kategori']}] {item['parametre']}: {relation or item['yorum']}"


def _build_ai_context(summary):
    actions = summary.get("gunluk_inceleme", [])
    critical_actions = [item for item in actions if item.get("status") == "danger"]
    positive_actions = [item for item in actions if item.get("status") == "success"]
    parameter_summaries = summary.get("parametre_ozetleri", [])

    critical_lines = critical_actions[:6] or actions[:6]
    positive_lines = positive_actions[:4]
    stats_lines = []

    for item in parameter_summaries[:8]:
        stat_line = (
            f"- [{item['kategori']}] {item['parametre']}: "
            f"guncel {item['guncel_deger_gosterim']}, "
            f"ortalama {item['ortalama_gosterim']}, "
            f"maksimum {item['maksimum_gosterim']}, "
            f"minimum {item['minimum_gosterim']}"
        )
        if item.get("hedef_gosterim"):
            stat_line += f", hedef {item['hedef_gosterim']}"
        stats_lines.append(stat_line)

    lines = [
        "Toplanti Kurali:",
        f"- ISG / Kalite / Uretim gunu: {summary['toplanti_kurali']['isg_kalite_uretim']}",
        f"- Planlama gunu: {summary['toplanti_kurali']['planlama']}",
        f"- Toplam cikarim: {len(actions)}",
        f"- Kritik sayisi: {len(critical_actions)}",
        f"- Olumlu sayisi: {len(positive_actions)}",
        "",
        "Kritik Basliklar:",
        *[_format_action_line(item) for item in critical_lines],
    ]

    if positive_lines:
        lines.extend(["", "Olumlu Basliklar:", *[_format_action_line(item) for item in positive_lines]])

    if stats_lines:
        lines.extend(["", "Parametre Ozetleri:", *stats_lines])

    return "\n".join(lines)


def build_manager_prompt(summary):
    context = _build_ai_context(summary)
    return f"""
Sen bir uretim fabrikasinda sabah toplantisi yapan kidemli bir yoneticisin.

Asagidaki veriler onceden analiz edildi.
Kurallar zaten uygulanmis durumda.
Senin gorevin sabah toplantisi icin kisa bir gundem ve onun altinda kisa bir AI yorumu yazmak.

Kurallar:
- Hesaplama yapma.
- Sadece verilen kategori, parametre, karsilastirma ve yorumlari kullan.
- Satis, pazarlama, finans, gelir, stddev, mean, varyans gibi burada gecmeyen kavramlari ASLA uydurma.
- Tablo kurma, pipe isareti kullanma, markdown tablosu yazma.
- Cikti kisa, net ve yonetici dilinde olsun.
- En kritik 3 noktayi one cikar.
- 2 olumlu noktayi da belirt.
- Turkce yaz.
- Kisa ve profesyonel ol.
- Su baslik duzenini aynen kullan:
## Gundem
- Once ...
- Sonra ...
## AI Yorumu
- ...
- ...
## Takip Edilecek Olumlu Basliklar
- ...
- ...
## Hemen Karar Verin
1. ...
2. ...
3. ...

Veri:
{context}
"""


def build_fallback_comment(summary):
    actions = summary.get("gunluk_inceleme", [])
    critical_actions = [item for item in actions if item.get("status") == "danger"]
    positive_actions = [item for item in actions if item.get("status") == "success"]
    toplanti = summary.get("toplanti_kurali", {})

    agenda_lines = [
        "## Gundem",
        (
            f"- Once ISG / Kalite / Uretim icin {toplanti.get('isg_kalite_uretim', '-')} tarihli kritik sapmalari ele alin."
        ),
        (
            f"- Sonra Planlama tarafinda {toplanti.get('planlama', '-')} tarihli stok ve dokum eksigi basliklarini netlestirin."
        ),
        f"- Toplam {len(actions)} cikarim var; {len(critical_actions)} kritik ve {len(positive_actions)} olumlu baslik bulunuyor.",
    ]

    reason_lines = ["## AI Yorumu"]
    if critical_actions:
        for item in critical_actions[:2]:
            reason_lines.append(
                f"- {item['kategori']} tarafinda {item['parametre']} icin {item['karsilastirma']} goruluyor. {item['yorum']}"
            )
    else:
        reason_lines.append(
            "- Kritik bir sapma yok; toplantida mevcut performansi koruyacak gunluk takip basliklari ele alinabilir."
        )

    positive_lines = ["## Takip Edilecek Olumlu Basliklar"]
    for item in positive_actions[:2]:
        positive_lines.append(
            f"- {item['parametre']} icin {item['karsilastirma']} sonucu var. {item['yorum']}"
        )
    if len(positive_lines) == 1:
        positive_lines.append("- Belirgin olumlu bir baslik yok; mevcut denge korunmali.")

    action_lines = ["## Hemen Karar Verin"]
    if critical_actions:
        for index, item in enumerate(critical_actions[:3], start=1):
            action_lines.append(f"{index}. {item['parametre']}: {item['yorum']} ({item['karsilastirma']})")
    else:
        action_lines.append("1. Kritik bir baslik yok; mevcut iyi performansi koruyacak gunluk takip surdurulmeli.")

    return "\n".join(agenda_lines + [""] + reason_lines + [""] + positive_lines + [""] + action_lines)


def is_unusable_ai_comment(text, summary):
    normalized = _normalize_text(text)
    if not normalized:
        return True

    if normalized.count("|") >= 2:
        return True

    if any(term in normalized for term in FORBIDDEN_AI_TERMS):
        return True

    required_headers = [
        "gundem",
        "ai yorumu",
        "takip edilecek olumlu basliklar",
        "hemen karar verin",
    ]
    if not all(header in normalized for header in required_headers):
        return True

    known_tokens = []
    for item in summary.get("gunluk_inceleme", [])[:10]:
        known_tokens.append(_normalize_text(item.get("kategori")))
        known_tokens.append(_normalize_text(item.get("parametre")))

    known_tokens = [token for token in known_tokens if token]
    if known_tokens and not any(token in normalized for token in known_tokens[:8]):
        return True

    return False


def ask_ollama(prompt):
    response = requests.post(
        f"{OLLAMA_BASE_URL}/api/generate",
        json={
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.2,
            },
        },
        timeout=120
    )
    response.raise_for_status()
    data = response.json()
    return data.get("response", "").strip()
