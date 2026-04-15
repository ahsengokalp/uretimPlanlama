import hashlib
import json
from datetime import date
from uuid import UUID
from uuid import uuid4

import pandas as pd

from analysis_engine import infer_unit
from config_loader import get_database_settings

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
    return get_database_settings()


def _missing_db_keys(settings):
    return [key for key, value in settings.items() if not value]


def _table_exists(connection, table_name, schema_name="public"):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name = %s
            )
            """,
            (schema_name, table_name),
        )
        return bool(cursor.fetchone()[0])


def _column_exists(connection, table_name, column_name, schema_name="public"):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                  AND column_name = %s
            )
            """,
            (schema_name, table_name, column_name),
        )
        return bool(cursor.fetchone()[0])


def _index_exists(connection, index_name, schema_name="public"):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = %s
                  AND indexname = %s
            )
            """,
            (schema_name, index_name),
        )
        return bool(cursor.fetchone()[0])


def _ensure_tables(connection):
    global _TABLES_READY
    if _TABLES_READY:
        return

    with connection.cursor() as cursor:
        if not _table_exists(connection, "manual_data_template_rows"):
            cursor.execute(
                """
                CREATE TABLE manual_data_template_rows (
                    id BIGSERIAL PRIMARY KEY,
                    row_order INTEGER NOT NULL,
                    kategori TEXT NOT NULL,
                    parametre TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        if not _index_exists(connection, "idx_manual_data_template_rows_order"):
            cursor.execute(
                """
                CREATE UNIQUE INDEX idx_manual_data_template_rows_order
                ON manual_data_template_rows (row_order)
                """
            )
        if not _index_exists(connection, "idx_manual_data_template_rows_pair"):
            cursor.execute(
                """
                CREATE UNIQUE INDEX idx_manual_data_template_rows_pair
                ON manual_data_template_rows (kategori, parametre)
                """
            )

        if not _table_exists(connection, "manual_data_submissions"):
            cursor.execute(
                """
                CREATE TABLE manual_data_submissions (
                    id UUID PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    source_kind TEXT NOT NULL,
                    template_name TEXT,
                    submission_name TEXT,
                    submission_hash TEXT,
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
        if not _column_exists(connection, "manual_data_submissions", "submission_name"):
            cursor.execute(
                """
                ALTER TABLE manual_data_submissions
                ADD COLUMN submission_name TEXT
                """
            )
        if not _column_exists(connection, "manual_data_submissions", "submission_hash"):
            cursor.execute(
                """
                ALTER TABLE manual_data_submissions
                ADD COLUMN submission_hash TEXT
                """
            )
        if not _index_exists(connection, "idx_manual_data_submissions_hash"):
            cursor.execute(
                """
                CREATE INDEX idx_manual_data_submissions_hash
                ON manual_data_submissions (submission_hash, created_at DESC)
                """
            )

        if not _table_exists(connection, "manual_data_values"):
            cursor.execute(
                """
                CREATE TABLE manual_data_values (
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
        if not _index_exists(connection, "idx_manual_data_values_submission"):
            cursor.execute(
                """
                CREATE INDEX idx_manual_data_values_submission
                ON manual_data_values (submission_id)
                """
            )
        if not _index_exists(connection, "idx_manual_data_values_lookup"):
            cursor.execute(
                """
                CREATE INDEX idx_manual_data_values_lookup
                ON manual_data_values (kategori, parametre, tarih)
                """
            )

    _TABLES_READY = True


def _normalize_template_rows(rows):
    normalized_rows = []
    seen_pairs = set()

    for row in rows or []:
        if not isinstance(row, dict):
            continue

        category = str(row.get("category") or row.get("kategori") or "").strip()
        parameter = str(row.get("parameter") or row.get("parametre") or "").strip()
        if not category or not parameter:
            continue

        pair_key = (category, parameter)
        if pair_key in seen_pairs:
            continue

        seen_pairs.add(pair_key)
        normalized_rows.append(
            {
                "row_order": len(normalized_rows) + 1,
                "category": category,
                "parameter": parameter,
            }
        )

    return normalized_rows


def list_manual_template_rows():
    if psycopg is None:
        return {
            "ok": False,
            "message": "Sabit satirlar okunamadi: 'psycopg' paketi kurulu degil.",
            "rows": [],
        }

    settings = _get_db_settings()
    missing_keys = _missing_db_keys(settings)
    if missing_keys:
        return {
            "ok": False,
            "message": f"Sabit satirlar okunamadi: eksik ortam degiskenleri {', '.join(missing_keys)}.",
            "rows": [],
        }

    try:
        with psycopg.connect(**settings) as connection:
            _ensure_tables(connection)
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT row_order, kategori, parametre
                    FROM manual_data_template_rows
                    ORDER BY row_order ASC, id ASC
                    """
                )
                rows = cursor.fetchall()
    except Exception as exc:
        return {
            "ok": False,
            "message": f"Sabit satirlar okunamadi: {exc}",
            "rows": [],
        }

    return {
        "ok": True,
        "rows": [
            {
                "row_order": row[0],
                "category": row[1],
                "parameter": row[2],
            }
            for row in rows
        ],
    }


def seed_manual_template_rows(rows):
    if psycopg is None:
        return {
            "ok": False,
            "message": "Sabit satirlar veri tabanina yazilamadi: 'psycopg' paketi kurulu degil.",
            "row_count": 0,
        }

    settings = _get_db_settings()
    missing_keys = _missing_db_keys(settings)
    if missing_keys:
        return {
            "ok": False,
            "message": f"Sabit satirlar veri tabanina yazilamadi: eksik ortam degiskenleri {', '.join(missing_keys)}.",
            "row_count": 0,
        }

    normalized_rows = _normalize_template_rows(rows)
    if not normalized_rows:
        return {
            "ok": False,
            "message": "Sabit satir listesi bos oldugu icin veri tabanina yazilamadi.",
            "row_count": 0,
        }

    try:
        with psycopg.connect(**settings) as connection:
            _ensure_tables(connection)
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM manual_data_template_rows")
                existing_count = int(cursor.fetchone()[0] or 0)
                if existing_count > 0:
                    return {
                        "ok": True,
                        "message": "Sabit satirlar zaten veri tabaninda mevcut.",
                        "row_count": existing_count,
                        "seeded": False,
                    }

                cursor.executemany(
                    """
                    INSERT INTO manual_data_template_rows (row_order, kategori, parametre)
                    VALUES (%s, %s, %s)
                    """,
                    [
                        (row["row_order"], row["category"], row["parameter"])
                        for row in normalized_rows
                    ],
                )
            connection.commit()
    except Exception as exc:
        return {
            "ok": False,
            "message": f"Sabit satirlar veri tabanina yazilamadi: {exc}",
            "row_count": 0,
        }

    return {
        "ok": True,
        "message": "Sabit satirlar veri tabanina yazildi.",
        "row_count": len(normalized_rows),
        "seeded": True,
    }


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
    normalized_payload_text = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    submission_hash = hashlib.sha256(normalized_payload_text.encode("utf-8")).hexdigest()
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
                    SELECT id
                    FROM manual_data_submissions
                    WHERE source_kind = %s
                      AND submission_hash = %s
                      AND created_at >= NOW() - INTERVAL '15 seconds'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    ("manual_form", submission_hash),
                )
                existing_row = cursor.fetchone()
                if existing_row:
                    return {
                        "ok": True,
                        "message": "Ayni veri zaten az once kaydedildi; mevcut kayit kullanildi.",
                        "submission_id": str(existing_row[0]),
                        "row_count": int(len(raw_df)),
                        "value_count": filled_value_count,
                        "duplicate": True,
                    }

                cursor.execute(
                    """
                    INSERT INTO manual_data_submissions (
                        id,
                        source_kind,
                        template_name,
                        submission_name,
                        submission_hash,
                        date_start,
                        date_end,
                        row_count,
                        filled_value_count,
                        payload,
                        operational_day,
                        planlama_day
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        submission_id,
                        "manual_form",
                        template_name,
                        submission_name,
                        submission_hash,
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

    payload = row[3]
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
