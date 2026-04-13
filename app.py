import json
import os
import re
from html import escape
from datetime import date

import pandas as pd
from flask import Flask, jsonify, render_template, request
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

from analysis_engine import analyze_raw_dataframe, infer_unit
from manual_entry_store import (
    delete_manual_submission,
    get_manual_submission_payload,
    list_manual_template_rows,
    list_recent_manual_submissions,
    save_manual_submission,
)
from ollama_client import (
    ask_ollama,
    build_fallback_comment,
    build_manager_prompt,
    is_unusable_ai_comment,
)

load_dotenv()

app = Flask(__name__)


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


def build_manual_template_config(ordered_rows, template_name=None, source="database"):
    config = {
        "path": None,
        "source": source,
        "template_name": template_name,
        "categories": [],
        "parameters_by_category": {},
        "row_index_map": {},
        "ordered_rows": [],
        "date_column_count": 0,
    }

    if not ordered_rows:
        return config

    categories = []
    parameters_by_category = {}
    row_index_map = {}
    normalized_rows = []

    for row_index, row in enumerate(ordered_rows, start=2):
        category_text = str(row.get("category") or "").strip()
        parameter_text = str(row.get("parameter") or "").strip()
        if not category_text or not parameter_text:
            continue
        if category_text not in categories:
            categories.append(category_text)
        parameters_by_category.setdefault(category_text, []).append(parameter_text)
        row_index_map[(category_text, parameter_text)] = row_index
        normalized_rows.append(
            {
                "category": category_text,
                "parameter": parameter_text,
            }
        )

    config["categories"] = categories
    config["parameters_by_category"] = parameters_by_category
    config["row_index_map"] = row_index_map
    config["ordered_rows"] = normalized_rows
    return config


def load_manual_template_config():
    db_result = list_manual_template_rows()
    if db_result.get("ok") and db_result.get("rows"):
        return build_manual_template_config(
            db_result["rows"],
            template_name="Veri tabani sabit satirlari",
            source="database",
        )

    return build_manual_template_config([], template_name=None, source="database")


MANUAL_TEMPLATE_CONFIG = load_manual_template_config()


def normalize_manual_number(value, parametre=None):
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
        numeric_value = float(text)
    except ValueError as exc:
        raise ValueError(f"Gecersiz sayisal deger: {value}") from exc

    if infer_unit(parametre) == "%":
        return numeric_value / 100

    return numeric_value


def build_manual_submission_name(date_keys):
    if not date_keys:
        return "manuel_giris"

    start_key = str(date_keys[0])
    end_key = str(date_keys[-1])
    if start_key == end_key:
        return f"manuel_giris_{start_key}"
    return f"manuel_giris_{start_key}_{end_key}"


def format_manual_prefill_value(value, parametre=None):
    if value is None or pd.isna(value):
        return None

    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        text = str(value).strip()
        return text or None

    normalized_value = float(numeric_value)
    if infer_unit(parametre) == "%":
        normalized_value *= 100

    if abs(normalized_value - round(normalized_value)) < 1e-9:
        return str(int(round(normalized_value)))
    return f"{normalized_value:.2f}".rstrip("0").rstrip(".")


def build_manual_payload_from_excel(file_obj):
    raw_df = pd.read_excel(file_obj, sheet_name="Veriler")
    raw_df.columns = [str(col).strip() if not isinstance(col, pd.Timestamp) else col for col in raw_df.columns]

    if "Kategori" not in raw_df.columns or "Parametre" not in raw_df.columns:
        raise ValueError("Veriler sayfasinda 'Kategori' ve 'Parametre' sutunlari bulunamadi.")

    date_columns = []
    for col in raw_df.columns:
        if col in {"Kategori", "Parametre"}:
            continue
        parsed = pd.to_datetime(col, errors="coerce")
        if not pd.isna(parsed):
            date_columns.append((col, pd.Timestamp(parsed).normalize().date().isoformat()))

    if not date_columns:
        raise ValueError("Excel icinde veri satirlarini dolduracak tarih kolonu bulunamadi.")

    rows = []
    for _, row in raw_df.iterrows():
        category = str(row.get("Kategori") or "").strip()
        parameter = str(row.get("Parametre") or "").strip()
        if not category or not parameter:
            continue

        values = {}
        for source_column, date_key in date_columns:
            formatted_value = format_manual_prefill_value(row.get(source_column), parameter)
            if formatted_value is not None:
                values[date_key] = formatted_value

        if not values:
            continue

        rows.append(
            {
                "category": category,
                "parameter": parameter,
                "values": values,
            }
        )

    if not rows:
        raise ValueError("Excel icinde forma aktarilacak dolu veri satiri bulunamadi.")

    return {
        "dates": [date_key for _, date_key in date_columns],
        "rows": rows,
    }


def build_manual_dataframe(payload_text):
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise ValueError("Manuel giris verisi okunamadi.") from exc

    date_values = payload.get("dates") or []
    rows = payload.get("rows") or []

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

    allowed_pairs = set(MANUAL_TEMPLATE_CONFIG["row_index_map"].keys())
    seen_pairs = set()
    records = []
    for row_index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue

        category = str(row.get("category") or "").strip()
        parameter = str(row.get("parameter") or "").strip()
        values = row.get("values") or {}
        if not isinstance(values, dict):
            values = {}

        has_any_value_input = any(str(item or "").strip() for item in values.values())
        if not has_any_value_input:
            continue

        if not category or not parameter:
            raise ValueError(f"{row_index}. satirda kategori ve parametre zorunlu.")

        if allowed_pairs and (category, parameter) not in allowed_pairs:
            raise ValueError(f"{row_index}. satirdaki kategori / parametre sablonla uyusmuyor.")

        pair_key = (category, parameter)
        if pair_key in seen_pairs:
            raise ValueError(f"{row_index}. satirda ayni kategori / parametre ikinci kez kullanildi.")
        seen_pairs.add(pair_key)

        record = {"Kategori": category, "Parametre": parameter}
        filled_value_count = 0

        for date_key in normalized_date_keys:
            parsed_value = normalize_manual_number(values.get(date_key), parameter)
            record[date_key] = parsed_value
            if parsed_value is not None:
                filled_value_count += 1

        if filled_value_count == 0:
            raise ValueError(f"{row_index}. satirda en az bir tarih icin deger girilmeli.")

        records.append(record)

    if not records:
        raise ValueError("Analiz icin en az bir dolu satir gerekiyor.")

    return pd.DataFrame(records), build_manual_submission_name(normalized_date_keys)

def render_analysis_result(result, source_name=None, source_kind="Excel yukleme", persistence_result=None):
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
        report_today=format_tr_date(report_today),
        operational_day=result["operational_day"],
        planlama_day=result["planlama_day"],
        success_count=success_count,
        critical_count=critical_count,
        source_name=source_name,
        source_kind=source_kind,
        persistence_result=persistence_result,
    )


@app.route("/", methods=["GET"])
def home():
    history_result = list_recent_manual_submissions(limit=8)
    if MANUAL_TEMPLATE_CONFIG["ordered_rows"]:
        template_note = "Sabit satir yapisi veri tabanindan okunur; manuel girisler de dogrudan PostgreSQL veri tabanina kaydedilir."
    else:
        template_note = "Sabit satir yapisi henuz veri tabaninda tanimli degil. Bu nedenle manuel tablo satirlari listelenemiyor."

    return render_template(
        "index.html",
        template_rows=MANUAL_TEMPLATE_CONFIG["ordered_rows"],
        template_note=template_note,
        manual_history=history_result.get("records", []),
        manual_history_error=None if history_result.get("ok") else history_result.get("message"),
    )


@app.route("/help", methods=["GET"])
def help_page():
    return render_template("help.html")


@app.route("/prefill-excel", methods=["POST"])
def prefill_excel():
    if "file" not in request.files:
        return jsonify({"ok": False, "message": "Once bir Excel dosyasi sec."}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"ok": False, "message": "Dosya secilmedi."}), 400

    try:
        file.stream.seek(0)
        payload = build_manual_payload_from_excel(file)
        return jsonify(
            {
                "ok": True,
                "source_name": secure_filename(file.filename),
                "payload": payload,
            }
        )
    except Exception as exc:
        return jsonify(
            {
                "ok": False,
                "message": str(exc),
            }
        ), 400


@app.route("/manual-submissions/<submission_id>", methods=["GET", "DELETE"])
def get_manual_submission(submission_id):
    if request.method == "DELETE":
        result = delete_manual_submission(submission_id)
        if not result.get("ok"):
            status_code = 404 if result.get("message") == "Kayit bulunamadi." else 400
            return jsonify({"ok": False, "message": result.get("message")}), status_code
        return jsonify(result)

    result = get_manual_submission_payload(submission_id)
    if not result.get("ok"):
        status_code = 404 if result.get("message") == "Kayit bulunamadi." else 400
        return jsonify({"ok": False, "message": result.get("message")}), status_code
    return jsonify(result)


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
        result = analyze_raw_dataframe(raw_df)
        persistence_result = save_manual_submission(
            raw_df,
            payload_text,
            submission_name=preferred_name or "manuel_giris",
            template_name=MANUAL_TEMPLATE_CONFIG.get("template_name"),
            result=result,
        )
        return render_analysis_result(
            result,
            source_name=preferred_name or "manuel_giris",
            source_kind="Manuel giris",
            persistence_result=persistence_result,
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


if __name__ == "__main__":
    app.run(host=os.getenv("APP_HOST", "0.0.0.0"), port=int(os.getenv("APP_PORT", "5053")), debug=True)
