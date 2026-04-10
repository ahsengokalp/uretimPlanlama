import os
import re
from html import escape
from datetime import date

from flask import Flask, render_template, request
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

from analysis_engine import analyze_excel_file
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
        )
    except Exception as exc:
        return render_template(
            "error.html",
            title="Analiz tamamlanamadi",
            message="Dosya yuklendi ancak veriler beklenen yapida islenemedi.",
            detail=str(exc),
        ), 500


if __name__ == "__main__":
    app.run(host="172.16.49.50", port=5053, debug=True)
