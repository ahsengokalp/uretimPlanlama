import pandas as pd
import plotly.express as px


def normalize_columns(df):
    df.columns = [str(c).strip() if not isinstance(c, pd.Timestamp) else c for c in df.columns]
    return df


def prepare_dataframe(df):
    """
    Excel'in 'Veriler' sayfasındaki wide yapıyı long yapıya çevirir.
    Beklenen giriş:
    Kategori | Parametre | 2026-03-01 | 2026-03-02 | ...
    Çıktı:
    Tarih | Kategori | Parametre | Deger
    """
    df = normalize_columns(df)

    if "Kategori" not in df.columns or "Parametre" not in df.columns:
        raise ValueError("Veriler sayfasında 'Kategori' ve 'Parametre' sütunları bulunamadı.")

    # Kategori ve Parametre dışındaki kolonlardan tarih olanları bul
    candidate_columns = [col for col in df.columns if col not in ["Kategori", "Parametre"]]

    date_columns = []
    for col in candidate_columns:
        parsed = pd.to_datetime(col, errors="coerce")
        if not pd.isna(parsed):
            date_columns.append(col)

    if not date_columns:
        raise ValueError(
            f"Tarih sütunları bulunamadı. Bulunan sütunlar: {list(df.columns)}"
        )

    # wide -> long dönüşüm
    melted = df.melt(
        id_vars=["Kategori", "Parametre"],
        value_vars=date_columns,
        var_name="Tarih",
        value_name="Deger"
    )

    melted["Tarih"] = pd.to_datetime(melted["Tarih"], errors="coerce")
    melted["Deger"] = pd.to_numeric(melted["Deger"], errors="coerce")

    # boş değerleri at
    melted = melted.dropna(subset=["Tarih", "Deger"]).copy()

    # Hedef satırlarını ayrıca yakalamak için base_param çıkar
    melted["Parametre"] = melted["Parametre"].astype(str).str.strip()

    return melted


def attach_targets(df):
    """
    Hedef satırlarını normal parametre satırlarına bağlar.
    Örn:
    'Bronz Hurda %' için
    'Bronz Hurda Hedefi %' satırını hedef olarak eşler
    """
    temp = df.copy()

    def clean_base_name(param):
        p = str(param).strip()
        p_low = p.lower()

        replacements = [
            " hedefi %",
            " hedef %",
            " hedefi",
            " hedef"
        ]

        for r in replacements:
            if p_low.endswith(r):
                return p[: len(p) - len(r)].strip()

        return p.strip()

    temp["BaseParametre"] = temp["Parametre"].apply(clean_base_name)

    is_target = temp["Parametre"].str.lower().str.contains("hedef", na=False)

    target_df = temp[is_target].copy()
    actual_df = temp[~is_target].copy()

    target_df = target_df.rename(columns={"Deger": "Hedef"})
    target_df = target_df[["Kategori", "BaseParametre", "Tarih", "Hedef"]]

    actual_df = actual_df.merge(
        target_df,
        how="left",
        on=["Kategori", "BaseParametre", "Tarih"]
    )

    return actual_df


def get_meeting_dates(df):
    # İSG / Kalite / Üretim için veri olan en son gün
    operational_df = df[df["Kategori"].isin(["İSG", "Kalite", "Üretim", "Uretim"])].copy()
    operational_latest = operational_df["Tarih"].max().normalize()

    # Bu grupta bir önceki günü almak yerine, doğrudan veri olan son günü kullanıyoruz
    # Çünkü Excel'de bazı günler boş olabilir
    operational_day = operational_latest

    # Planlama için veri olan en son gün
    planlama_df = df[df["Kategori"].str.lower() == "planlama"].copy()
    planlama_day = planlama_df["Tarih"].max().normalize()

    return planlama_day, operational_day


def generate_planlama_action(parametre, value):
    p = str(parametre).strip().lower()

    rules = [
        ("fatura bekleyen hazır ton", 2, "Girişler neden aksıyor? Ne yapacağız?"),
        ("kalite kontrol bekleyen ton", 1, "Kalite Kontrol'de ürün beklememeli. Karar veremediğimiz bir nokta mı var?"),
        ("rework bekleyen ton", 1, "Rework'ler neden yürümüyor?"),
        ("taşlama stoğu ton", 20, "İşler yığıldı. 20 tonun altına inmek için ne yapacağız?"),
        ("kumlama stoğu ton", 20, "İşler yığıldı. 20 tonun altına inmek için ne yapacağız? Sarsakta mı yoksa kumlamada mı birikti?"),
        ("dökülecek iş miktarı ton", 50, "Dökümde yapılacak çok iş var. Hangi hatta döküm mesaisi yapacağız?"),
        ("mevcut sipariş ton", 20, "Kritik sınırı aştık. Durum bazında inceleme yapılmalı"),
        ("2 hafta sonrası için döküm eksiği ton", 30, "Kritik sınırı aştık. Hat ve alaşım bazında inceleme yapılmalı"),
        ("1 hafta sonrası için döküm eksiği ton", 10, "Kritik sınırı aştık. Hat ve alaşım bazında inceleme yapılmalı"),
        ("mevcut hafta için döküm eksiği ton", 5, "Kritik sınırı aştık. Hat, alaşım ve ürün bazında inceleme yapılmalı"),
        ("bakiye için döküm eksiği ton", 2, "Bu bakiye hurdadan mı geliyor yoksa planlamamızda mı bir hata var?"),
    ]

    for key, threshold, message in rules:
        if key in p and value > threshold:
            return message

    return None


def generate_isg_action(parametre, value):
    if "kaza" in str(parametre).lower():
        if value == 0:
            return "Böyle devam!"
        if value > 0:
            return "Kaza nedeni nedir? Aksiyon planla"
    return None


def generate_kalite_uretim_action(kategori, value, target):
    if pd.isna(target):
        return None

    kategori = str(kategori).lower()

    if kategori == "kalite":
        if value > target:
            return "Hatayı analiz et. Hangi hatta hangi üründe problem var? Aksiyon listesine yaz"
        return "Tebrikler. Hedefleneni gerçekleştirdin."

    if kategori in ["üretim", "uretim"]:
        if value > target:
            return "Tebrikler. Bunu nasıl sürdürebiliriz?"
        return "Hedefi neden tutturamadık? Arıza mı vardı? Personel eksiğin mi var? Yoksa verimsiz mi çalışıyoruz? Aksiyonumuz nedir?"

    return None


def create_charts(df):
    charts = []

    for kategori in df["Kategori"].dropna().unique():
        cat_df = df[df["Kategori"] == kategori].copy()

        fig = px.line(
            cat_df,
            x="Tarih",
            y="Deger",
            color="Parametre",
            markers=True,
            title=f"{kategori} - Günlük Trend",
        )

        charts.append({
            "title": f"{kategori} - Günlük Trend",
            "html": fig.to_html(full_html=False, include_plotlyjs="cdn")
        })

    return charts


def build_daily_review(df):
    planlama_day, operational_day = get_meeting_dates(df)

    review_rows = []

    prev_df = df[
        (df["Kategori"].isin(["İSG", "ISG", "Kalite", "Üretim", "Uretim"])) &
        (df["Tarih"].dt.normalize() == operational_day)
    ].copy()

    for _, row in prev_df.iterrows():
        kategori = str(row["Kategori"])
        yorum = None

        if kategori.lower() in ["i̇sg", "isg"]:
            yorum = generate_isg_action(row["Parametre"], row["Deger"])
        elif kategori.lower() in ["kalite", "üretim", "uretim"]:
            yorum = generate_kalite_uretim_action(
                kategori,
                row["Deger"],
                row["Hedef"] if "Hedef" in row.index else None
            )

        if yorum:
            review_rows.append({
                "inceleme_gunu": str(operational_day.date()),
                "kategori": kategori,
                "parametre": row["Parametre"],
                "deger": row["Deger"],
                "hedef": row["Hedef"] if "Hedef" in row.index else None,
                "yorum": yorum,
            })

    plan_df = df[
        (df["Kategori"].str.lower() == "planlama") &
        (df["Tarih"].dt.normalize() == planlama_day)
    ].copy()

    for _, row in plan_df.iterrows():
        yorum = generate_planlama_action(row["Parametre"], row["Deger"])
        if yorum:
            review_rows.append({
                "inceleme_gunu": str(planlama_day.date()),
                "kategori": row["Kategori"],
                "parametre": row["Parametre"],
                "deger": row["Deger"],
                "hedef": row["Hedef"] if "Hedef" in row.index else None,
                "yorum": yorum,
            })

    return review_rows, planlama_day, operational_day


def analyze_excel_file(filepath):
    # Özellikle Veriler sayfasını oku
    raw_df = pd.read_excel(filepath, sheet_name="Veriler")
    df = prepare_dataframe(raw_df)
    df = attach_targets(df)

    charts = create_charts(df)
    daily_review, planlama_day, operational_day = build_daily_review(df)

    actions = [row for row in daily_review if row["yorum"]]

    summary_for_ai = {
        "toplanti_kurali": {
            "isg_kalite_uretim": str(operational_day.date()),
            "planlama": str(planlama_day.date()),
        },
        "gunluk_inceleme": daily_review
    }

    info_text = (
        f"İSG / Kalite / Üretim için {operational_day.date()} verileri, "
        f"Planlama için {planlama_day.date()} verileri yorumlandı."
    )

    return {
        "charts": charts,
        "daily_review": daily_review,
        "actions": actions,
        "summary_for_ai": summary_for_ai,
        "info_text": info_text,
    }
