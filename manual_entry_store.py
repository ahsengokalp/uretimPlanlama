import json
import os
from datetime import date
from uuid import UUID
from uuid import uuid4

import pandas as pd

from analysis_engine import infer_unit

try:
    import psycopg
    from psycopg.types.json import Jsonb
except ImportError:  # pragma: no cover - handled at runtime when dependency is missing
    psycopg = None
    Jsonb = None


class ManualEntryStoreError(Exception):
    pass


_TABLES_READY = False


def _get_db_settings():
    return {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
    }


def _missing_db_keys(settings):
    return [key for key, value in settings.items() if not value]


def _ensure_tables(connection):
    global _TABLES_READY
    if _TABLES_READY:
        return

    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_data_submissions (
                id UUID PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                source_kind TEXT NOT NULL,
                template_name TEXT,
                submission_name TEXT,
                date_start DATE,
                date_end DATE,
                row_count INTEGER NOT NULL,
                filled_value_count INTEGER NOT NULL,
                payload JSONB NOT NULL,
                operational_day DATE,
                planlama_day DATE
            )
            """
        )
        cursor.execute(
            """
            ALTER TABLE manual_data_submissions
            ADD COLUMN IF NOT EXISTS submission_name TEXT
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_data_values (
                id BIGSERIAL PRIMARY KEY,
                submission_id UUID NOT NULL REFERENCES manual_data_submissions(id) ON DELETE CASCADE,
                row_order INTEGER NOT NULL,
                date_order INTEGER NOT NULL,
                kategori TEXT NOT NULL,
                parametre TEXT NOT NULL,
                tarih DATE NOT NULL,
                deger DOUBLE PRECISION NOT NULL,
                unit TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_manual_data_values_submission
            ON manual_data_values (submission_id)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_manual_data_values_lookup
            ON manual_data_values (kategori, parametre, tarih)
            """
        )

    _TABLES_READY = True


def save_manual_submission(raw_df, payload_text, submission_name, template_name=None, result=None):
    if psycopg is None or Jsonb is None:
        return {
            "ok": False,
            "message": "PostgreSQL kaydi yapilamadi: 'psycopg' paketi kurulu degil.",
        }

    settings = _get_db_settings()
    missing_keys = _missing_db_keys(settings)
    if missing_keys:
        return {
            "ok": False,
            "message": f"PostgreSQL kaydi yapilamadi: eksik ortam degiskenleri {', '.join(missing_keys)}.",
        }

    payload = json.loads(payload_text)
    date_columns = list(raw_df.columns[2:])
    filled_value_count = int(raw_df[date_columns].count().sum()) if date_columns else 0
    submission_id = uuid4()

    values = []
    for row_order, (_, row) in enumerate(raw_df.iterrows(), start=1):
        for date_order, date_key in enumerate(date_columns, start=1):
            value = row.get(date_key)
            if pd.isna(value):
                continue

            parsed_date = pd.to_datetime(date_key, errors="coerce")
            values.append(
                (
                    submission_id,
                    row_order,
                    date_order,
                    str(row["Kategori"]).strip(),
                    str(row["Parametre"]).strip(),
                    parsed_date.date() if not pd.isna(parsed_date) else date.fromisoformat(str(date_key)),
                    float(value),
                    infer_unit(row["Parametre"]),
                )
            )

    try:
        with psycopg.connect(**settings) as connection:
            _ensure_tables(connection)
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO manual_data_submissions (
                        id,
                        source_kind,
                        template_name,
                        submission_name,
                        date_start,
                        date_end,
                        row_count,
                        filled_value_count,
                        payload,
                        operational_day,
                        planlama_day
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        submission_id,
                        "manual_form",
                        template_name,
                        submission_name,
                        pd.to_datetime(date_columns[0]).date() if date_columns else None,
                        pd.to_datetime(date_columns[-1]).date() if date_columns else None,
                        int(len(raw_df)),
                        filled_value_count,
                        Jsonb(payload),
                        pd.to_datetime(result["operational_day"]).date() if result and result.get("operational_day") else None,
                        pd.to_datetime(result["planlama_day"]).date() if result and result.get("planlama_day") else None,
                    ),
                )
                if values:
                    cursor.executemany(
                        """
                        INSERT INTO manual_data_values (
                            submission_id,
                            row_order,
                            date_order,
                            kategori,
                            parametre,
                            tarih,
                            deger,
                            unit
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        values,
                    )
            connection.commit()
    except Exception as exc:
        return {
            "ok": False,
            "message": f"PostgreSQL kaydi yapilamadi: {exc}",
        }

    return {
        "ok": True,
        "message": "Manuel giris verileri PostgreSQL veri tabanina kaydedildi.",
        "submission_id": str(submission_id),
        "row_count": int(len(raw_df)),
        "value_count": filled_value_count,
    }


def list_recent_manual_submissions(limit=8):
    if psycopg is None:
        return {
            "ok": False,
            "message": "Kayit gecmisi okunamadi: 'psycopg' paketi kurulu degil.",
            "records": [],
        }

    settings = _get_db_settings()
    missing_keys = _missing_db_keys(settings)
    if missing_keys:
        return {
            "ok": False,
            "message": f"Kayit gecmisi okunamadi: eksik ortam degiskenleri {', '.join(missing_keys)}.",
            "records": [],
        }

    try:
        with psycopg.connect(**settings) as connection:
            _ensure_tables(connection)
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        id,
                        submission_name,
                        created_at,
                        date_start,
                        date_end,
                        row_count,
                        filled_value_count,
                        operational_day,
                        planlama_day
                    FROM manual_data_submissions
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (int(limit),),
                )
                rows = cursor.fetchall()
    except Exception as exc:
        return {
            "ok": False,
            "message": f"Kayit gecmisi okunamadi: {exc}",
            "records": [],
        }

    records = []
    for row in rows:
        created_at = row[2]
        date_start = row[3]
        date_end = row[4]
        if date_start and date_end:
            if date_start == date_end:
                date_range_label = date_start.strftime("%d.%m.%Y")
            else:
                date_range_label = f"{date_start.strftime('%d.%m.%Y')} - {date_end.strftime('%d.%m.%Y')}"
        else:
            date_range_label = "Tarih araligi yok"

        records.append(
            {
                "id": str(row[0]),
                "submission_name": row[1],
                "display_name": created_at.strftime("%d.%m.%Y %H:%M") if created_at else "Tarihsiz kayit",
                "created_at": created_at.isoformat() if created_at else None,
                "created_at_label": created_at.strftime("%d.%m.%Y %H:%M") if created_at else "-",
                "date_start": date_start.isoformat() if date_start else None,
                "date_end": date_end.isoformat() if date_end else None,
                "date_range_label": date_range_label,
                "row_count": row[5],
                "filled_value_count": row[6],
                "operational_day": row[7].isoformat() if row[7] else None,
                "planlama_day": row[8].isoformat() if row[8] else None,
            }
        )

    return {
        "ok": True,
        "records": records,
    }


def get_manual_submission_payload(submission_id):
    if psycopg is None:
        return {
            "ok": False,
            "message": "Kayit yuklenemedi: 'psycopg' paketi kurulu degil.",
        }

    settings = _get_db_settings()
    missing_keys = _missing_db_keys(settings)
    if missing_keys:
        return {
            "ok": False,
            "message": f"Kayit yuklenemedi: eksik ortam degiskenleri {', '.join(missing_keys)}.",
        }

    try:
        normalized_id = str(UUID(str(submission_id)))
    except ValueError:
        return {
            "ok": False,
            "message": "Kayit kimligi gecersiz.",
        }

    try:
        with psycopg.connect(**settings) as connection:
            _ensure_tables(connection)
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, submission_name, created_at, payload
                    FROM manual_data_submissions
                    WHERE id = %s
                    """,
                    (normalized_id,),
                )
                row = cursor.fetchone()
    except Exception as exc:
        return {
            "ok": False,
            "message": f"Kayit yuklenemedi: {exc}",
        }

    if not row:
        return {
            "ok": False,
            "message": "Kayit bulunamadi.",
        }

    payload = row[2]
    if isinstance(payload, str):
        payload = json.loads(payload)

    return {
        "ok": True,
        "record": {
            "id": str(row[0]),
            "submission_name": row[1],
            "display_name": row[2].strftime("%d.%m.%Y %H:%M") if row[2] else "Tarihsiz kayit",
            "payload": payload,
        },
    }


def delete_manual_submission(submission_id):
    if psycopg is None:
        return {
            "ok": False,
            "message": "Kayit silinemedi: 'psycopg' paketi kurulu degil.",
        }

    settings = _get_db_settings()
    missing_keys = _missing_db_keys(settings)
    if missing_keys:
        return {
            "ok": False,
            "message": f"Kayit silinemedi: eksik ortam degiskenleri {', '.join(missing_keys)}.",
        }

    try:
        normalized_id = str(UUID(str(submission_id)))
    except ValueError:
        return {
            "ok": False,
            "message": "Kayit kimligi gecersiz.",
        }

    try:
        with psycopg.connect(**settings) as connection:
            _ensure_tables(connection)
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM manual_data_submissions
                    WHERE id = %s
                    """,
                    (normalized_id,),
                )
                deleted_count = cursor.rowcount
            connection.commit()
    except Exception as exc:
        return {
            "ok": False,
            "message": f"Kayit silinemedi: {exc}",
        }

    if not deleted_count:
        return {
            "ok": False,
            "message": "Kayit bulunamadi.",
        }

    return {
        "ok": True,
        "message": "Kayit veri tabanindan silindi.",
        "submission_id": normalized_id,
    }
