import json
import os
import re
from html import escape
from datetime import date, datetime

import pandas as pd
from flask import Flask, render_template, request
from flask import abort, send_from_directory
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

from analysis_engine import analyze_excel_file, analyze_raw_dataframe
from ollama_client import (
    ask_ollama,
    build_fallback_comment,
    build_manager_prompt,
    is_unusable_ai_comment,
)

load_dotenv()

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)


TR_MONTHS = {
    1: "Ocak",
    2: "Subat",
    3: "Mart",
    4: "Nisan",
    5: "Mayis",
    6: "Haziran",
    7: "Temmuz",
    8: "Agustos",
    9: "Eylul",
    10: "Ekim",
    11: "Kasim",
    12: "Aralik",
}


def format_tr_date(value):
    return f"{value.day} {TR_MONTHS[value.month]} {value.year}"


def _format_inline_markdown(text):
    text = escape(text)
    return re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)


def _split_heading_content(text):
    patterns = [
        r"\s+(?=Bu\b)",
        r"\s+(?=Genel\b)",
        r"\s+(?=Personel\b)",
        r"\s+(?=Makine\b)",
        r"\s+(?=Insan\b)",
        r"\s+(?=Operasyon\b)",
        r"\s+(?=Uretim\b)",
        r"\s+(?=Ozet\b)",
        r"\s+(?=Trend\b)",
        r"\s+(?=\*\*)",
        r"\s+(?=\d+\.)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return text[: match.start()].strip(), text[match.start() :].strip()
    return text.strip(), None


def render_ai_comment_html(text):
    if not text:
        return None

    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = re.sub(r"\s+(#{2,3}\s+)", r"\n\1", normalized)
    normalized = re.sub(r"\s+(\d+\.\s+\*\*)", r"\n\1", normalized)
    normalized = normalized.replace(" --- ", "\n")
    normalized = normalized.replace("---", "\n")

    lines = [line.strip() for line in normalized.split("\n") if line.strip()]
    parts = []
    list_mode = None

    def close_list():
        nonlocal list_mode
        if list_mode:
            parts.append(f"</{list_mode}>")
            list_mode = None

    for line in lines:
        if line.startswith("### "):
            close_list()
            heading, rest = _split_heading_content(line[4:])
            parts.append(f"<h4>{_format_inline_markdown(heading)}</h4>")
            if rest:
                parts.append(f"<p>{_format_inline_markdown(rest)}</p>")
            continue

        if line.startswith("## "):
            close_list()
            heading, rest = _split_heading_content(line[3:])
            parts.append(f"<h3>{_format_inline_markdown(heading)}</h3>")
            if rest:
                parts.append(f"<p>{_format_inline_markdown(rest)}</p>")
            continue

        if re.match(r"^(\-|\*|•)\s+", line):
            if list_mode != "ul":
                close_list()
                list_mode = "ul"
                parts.append("<ul>")
            item_text = re.sub(r"^(\-|\*|•)\s+", "", line)
            parts.append(f"<li>{_format_inline_markdown(item_text)}</li>")
            continue

        if re.match(r"^\d+\.\s+", line):
            if list_mode != "ol":
                close_list()
                list_mode = "ol"
                parts.append("<ol>")
            item_text = re.sub(r"^\d+\.\s+", "", line)
            parts.append(f"<li>{_format_inline_markdown(item_text)}</li>")
            continue

        close_list()
        parts.append(f"<p>{_format_inline_markdown(line)}</p>")

    close_list()
    return "".join(parts)


def normalize_manual_number(value):
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    text = text.replace("%", "").replace(" ", "")
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    else:
        text = text.replace(",", ".")

    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(f"Gecersiz sayisal deger: {value}") from exc


def build_manual_dataframe(payload_text):
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise ValueError("Manuel giris verisi okunamadi.") from exc

    date_values = payload.get("dates") or []
    rows = payload.get("rows") or []
    preferred_name = secure_filename(str(payload.get("filename") or "").strip())

    if not isinstance(date_values, list) or not isinstance(rows, list):
        raise ValueError("Manuel giris verisi beklenen formatta degil.")

    normalized_date_keys = []
    seen_dates = set()
    for raw_date in date_values:
        text = str(raw_date or "").strip()
        if not text:
            continue

        parsed = pd.to_datetime(text, errors="coerce")
        if pd.isna(parsed):
            raise ValueError(f"Gecersiz tarih: {raw_date}")

        normalized_key = pd.Timestamp(parsed).normalize().date().isoformat()
        if normalized_key not in seen_dates:
            normalized_date_keys.append(normalized_key)
            seen_dates.add(normalized_key)

    if not normalized_date_keys:
        raise ValueError("En az bir tarih girilmeli.")

    records = []
    for row_index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue

        category = str(row.get("category") or "").strip()
        parameter = str(row.get("parameter") or "").strip()
        values = row.get("values") or {}
        if not isinstance(values, dict):
            values = {}

        has_any_input = category or parameter or any(str(item or "").strip() for item in values.values())
        if not has_any_input:
            continue

        if not category or not parameter:
            raise ValueError(f"{row_index}. satirda kategori ve parametre zorunlu.")

        record = {"Kategori": category, "Parametre": parameter}
        filled_value_count = 0

        for date_key in normalized_date_keys:
            parsed_value = normalize_manual_number(values.get(date_key))
            record[date_key] = parsed_value
            if parsed_value is not None:
                filled_value_count += 1

        if filled_value_count == 0:
            raise ValueError(f"{row_index}. satirda en az bir tarih icin deger girilmeli.")

        records.append(record)

    if not records:
        raise ValueError("Analiz icin en az bir dolu satir gerekiyor.")

    return pd.DataFrame(records), preferred_name


def create_generated_excel(raw_df, preferred_name=None):
    base_name = preferred_name or f"manuel_giris_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    filename = base_name if base_name.lower().endswith(".xlsx") else f"{base_name}.xlsx"
    filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)

    duplicate_index = 1
    while os.path.exists(filepath):
        stem = filename[:-5] if filename.lower().endswith(".xlsx") else filename
        filename = f"{stem}_{duplicate_index}.xlsx"
        filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        duplicate_index += 1

    raw_df.to_excel(filepath, sheet_name="Veriler", index=False)
    return filename, filepath


def render_analysis_result(result, source_name=None, source_kind="Excel yukleme", generated_filename=None):
    report_today = date.today()
    success_count = len([item for item in result["actions"] if item["status"] == "success"])
    critical_count = len([item for item in result["actions"] if item["status"] == "danger"])

    ai_comment = None
    ai_comment_html = None
    if result["summary_for_ai"]:
        prompt = build_manager_prompt(result["summary_for_ai"])
        try:
            ai_comment = ask_ollama(prompt)
            if is_unusable_ai_comment(ai_comment, result["summary_for_ai"]):
                ai_comment = build_fallback_comment(result["summary_for_ai"])
        except Exception:
            ai_comment = build_fallback_comment(result["summary_for_ai"])
        ai_comment_html = render_ai_comment_html(ai_comment)

    return render_template(
        "result.html",
        charts=result["charts"],
        daily_review=result["daily_review"],
        actions=result["actions"],
        highlight_actions=result["highlight_actions"],
        parameter_summaries=result["parameter_summaries"],
        ai_comment=ai_comment,
        ai_comment_html=ai_comment_html,
        info_text=result["info_text"],
        report_today=format_tr_date(report_today),
        operational_day=result["operational_day"],
        planlama_day=result["planlama_day"],
        success_count=success_count,
        critical_count=critical_count,
        source_name=source_name,
        source_kind=source_kind,
        generated_filename=generated_filename,
    )


@app.route("/", methods=["GET"])
def home():
    return render_template("index.html")


@app.route("/analyze", methods=["POST"])
def analyze():
    if "file" not in request.files:
        return render_template(
            "error.html",
            title="Dosya bulunamadi",
            message="Analizi baslatmak icin once bir Excel dosyasi yukleyin.",
            detail="Desteklenen bicimler: .xlsx ve .xls",
        ), 400

    file = request.files["file"]

    if file.filename == "":
        return render_template(
            "error.html",
            title="Dosya secilmedi",
            message="Devam edebilmek icin analiz etmek istediginiz Excel dosyasini secin.",
            detail="Ilk iki sutun Kategori ve Parametre, diger sutunlar tarih olmali.",
        ), 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    file.save(filepath)

    try:
        result = analyze_excel_file(filepath)
        return render_analysis_result(result, source_name=filename, source_kind="Excel yukleme")
    except Exception as exc:
        return render_template(
            "error.html",
            title="Analiz tamamlanamadi",
            message="Dosya yuklendi ancak veriler beklenen yapida islenemedi.",
            detail=str(exc),
        ), 500


@app.route("/analyze-manual", methods=["POST"])
def analyze_manual():
    payload_text = request.form.get("manual_payload", "")
    if not payload_text.strip():
        return render_template(
            "error.html",
            title="Manuel giris bos",
            message="Analizi baslatmak icin once manuel veri tablosunu doldurun.",
            detail="En az bir tarih ve en az bir veri satiri girilmeli.",
        ), 400

    try:
        raw_df, preferred_name = build_manual_dataframe(payload_text)
        generated_filename, _ = create_generated_excel(raw_df, preferred_name)
        result = analyze_raw_dataframe(raw_df)
        return render_analysis_result(
            result,
            source_name=generated_filename,
            source_kind="Manuel giris",
            generated_filename=generated_filename,
        )
    except ValueError as exc:
        return render_template(
            "error.html",
            title="Manuel giris dogrulanamadi",
            message="Girdiginiz veriler analiz icin hazir degil.",
            detail=str(exc),
        ), 400
    except Exception as exc:
        return render_template(
            "error.html",
            title="Manuel analiz tamamlanamadi",
            message="Veriler kaydedildi ancak analiz asamasinda bir sorun olustu.",
            detail=str(exc),
        ), 500


@app.route("/downloads/<path:filename>", methods=["GET"])
def download_file(filename):
    safe_filename = secure_filename(filename)
    if safe_filename != filename:
        abort(404)

    return send_from_directory(app.config["UPLOAD_FOLDER"], safe_filename, as_attachment=True)


if __name__ == "__main__":
    app.run(host=os.getenv("APP_HOST", "0.0.0.0"), port=int(os.getenv("APP_PORT", "5053")), debug=True)
