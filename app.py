import os
from flask import Flask, render_template, request
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

from analyzer import analyze_excel_file
from ollama_client import ask_ollama, build_manager_prompt

load_dotenv()

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)


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

        ai_comment = None
        if result["daily_review"]:
            prompt = build_manager_prompt(result["summary_for_ai"])
            try:
                ai_comment = ask_ollama(prompt)
            except Exception:
                ai_comment = None

        return render_template(
            "result.html",
            charts=result["charts"],
            daily_review=result["daily_review"],
            actions=result["actions"],
            ai_comment=ai_comment,
            info_text=result["info_text"],
        )
    except Exception as exc:
        return render_template(
            "error.html",
            title="Analiz tamamlanamadi",
            message="Dosya yuklendi ancak veriler beklenen yapida islenemedi.",
            detail=str(exc),
        ), 500


if __name__ == "__main__":
    app.run(debug=True)
