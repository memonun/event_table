#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bu betik, Bugece API’sinden etkinlikleri çekip Supabase/PostgreSQL
tablolarına upsert yapan ve fiyat değişimlerini ayrıntılı olarak
“history” tablosuna kaydeden üretim-hazır bir örnektir.
"""

import os
from datetime import datetime

import requests
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

# --------------------------------------------------------------------------- #
# 1. Ortam değişkenleri & veritabanı bağlantısı
# --------------------------------------------------------------------------- #
load_dotenv()                                   # .env içinden DATABASE_URL al
DATABASE_URL = os.getenv("DATABASE_URL")

def connect_db():
    """Supabase TLS gerektirdiği için sslmode='require' parametresi ile bağlan."""
    return psycopg2.connect(DATABASE_URL, sslmode="require")

# --------------------------------------------------------------------------- #
# 2. Bugece’yi çekip normalize eden yardımcı fonksiyonlar
# --------------------------------------------------------------------------- #
EVENT_SOURCE_URL = (
    "https://barac.bugece.co/v1/event/list"
    "?country=298795&city=&category=&start=&end=&venue=&search="
    "&pageSize=1000&sortBy=popularity&sortDir=desc"
)
HEADERS = {"User-Agent": "Mozilla/5.0"}

def fetch_events():
    """API’den ham JSON’u çeker, timeout ekler, HTTP hatalarında exception atar."""
    resp = requests.get(EVENT_SOURCE_URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.json().get("data", {}).get("items", [])

def normalize_event(raw: dict) -> dict:
    """Ham API çıktısını veritabanına uygun hâle getirir."""
    price_list = raw.get("price_list", [])
    for item in price_list:
        # İsim → kategori, sold_out boolean'ı türet
        item["category"] = item.pop("name", "")
        item["sold_out"] = not item.pop("status", True) if "status" in item else False

    return {
        "provider": "Bugece",
        "name": raw.get("name", "Unknown"),
        "venue": raw.get("venue", {}).get("name", "Unknown"),
        "date":  raw.get("date",  "Unknown"),
        "genre": "",
        "price_list": price_list,
    }

# --------------------------------------------------------------------------- #
# 3. Upsert + fiyat geçmişi yönetimi
# --------------------------------------------------------------------------- #
def upsert_event_with_history(event: dict) -> None:
    """
    * Etkinlik var mı? Yoksa ekle, varsa güncelle.
    * Aktif fiyatlar ile gelen listeyi karşılaştır:
        - Değişenler  → history'ye 'UPDATED', satırı güncelle.
        - Yeniler      → prices & history'ye 'ADDED'.
        - Silinenler   → prices.is_active = FALSE, history'ye 'REMOVED'.
    """
    now = datetime.now()

    with connect_db() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:

        # ---- 3.1 Etkinlik upsert ------------------------------------------------
        cur.execute(
            """
            SELECT id FROM bugece_events
            WHERE name = %(name)s AND venue = %(venue)s AND date = %(date)s
            """,
            {"name": event["name"], "venue": event["venue"], "date": event["date"]}
        )
        row = cur.fetchone()

        if row:  # güncelle
            event_id = row["id"]
            cur.execute(
                """
                UPDATE bugece_events
                SET provider = %(provider)s,
                    genre    = %(genre)s,
                    last_seen= %(now)s
                WHERE id = %(id)s
                """,
                {"provider": event["provider"], "genre": event["genre"], "now": now, "id": event_id}
            )
        else:    # yeni kayıt
            cur.execute(
                """
                INSERT INTO bugece_events
                    (provider, name, venue, date, genre, created_at, last_seen)
                VALUES
                    (%(provider)s, %(name)s, %(venue)s, %(date)s, %(genre)s, %(now)s, %(now)s)
                RETURNING id
                """,
                {**event, "now": now}
            )
            event_id = cur.fetchone()["id"]

        # ---- 3.2 Aktif fiyatları çek -------------------------------------------
        cur.execute(
            """
            SELECT id, category, price, sold_out
            FROM bugece_prices
            WHERE event_id = %(eid)s AND is_active = TRUE
            """,
            {"eid": event_id}
        )
        existing = {r["category"]: r for r in cur.fetchall()}
        seen_categories = set()

        # ---- 3.3 Yeni gelen listeyi işle ---------------------------------------
        for p in event["price_list"]:
            cat, price_val, sold_out = p["category"], p["price"], p["sold_out"]
            seen_categories.add(cat)

            if cat in existing:                               # muhtemel UPDATE
                prev = existing[cat]
                if (prev["price"], prev["sold_out"]) != (price_val, sold_out):
                    # history
                    cur.execute(
                        """
                        INSERT INTO bugece_price_history
                            (event_id, category, price, sold_out, change_date, change_type)
                        VALUES
                            (%(eid)s, %(cat)s, %(old_price)s, %(old_so)s, %(now)s, 'UPDATED')
                        """,
                        {
                            "eid": event_id, "cat": cat,
                            "old_price": prev["price"], "old_so": prev["sold_out"], "now": now
                        }
                    )
                    # update
                    cur.execute(
                        """
                        UPDATE bugece_prices
                        SET price = %(price)s,
                            sold_out = %(sold_out)s,
                            last_seen = %(now)s
                        WHERE id = %(id)s
                        """,
                        {
                            "price": price_val, "sold_out": sold_out,
                            "now": now, "id": prev["id"]
                        }
                    )
            else:                                              # INSERT + history
                cur.execute(
                    """
                    INSERT INTO bugece_prices
                        (event_id, category, price, sold_out,
                         created_at, last_seen, is_active)
                    VALUES
                        (%(eid)s, %(cat)s, %(price)s, %(sold_out)s,
                         %(now)s, %(now)s, TRUE)
                    RETURNING id
                    """,
                    {
                        "eid": event_id, "cat": cat,
                        "price": price_val, "sold_out": sold_out, "now": now
                    }
                )
                cur.execute(
                    """
                    INSERT INTO bugece_price_history
                        (event_id, category, price, sold_out, change_date, change_type)
                    VALUES
                        (%(eid)s, %(cat)s, %(price)s, %(sold_out)s, %(now)s, 'ADDED')
                    """,
                    {
                        "eid": event_id, "cat": cat,
                        "price": price_val, "sold_out": sold_out, "now": now
                    }
                )

        # ---- 3.4 Listede artık olmayan kategorileri pasifleştir ----------------
        for cat, rec in existing.items():
            if cat not in seen_categories:
                cur.execute(
                    """
                    UPDATE bugece_prices
                    SET is_active = FALSE, last_seen = %(now)s
                    WHERE id = %(id)s
                    """,
                    {"now": now, "id": rec["id"]}
                )
                cur.execute(
                    """
                    INSERT INTO bugece_price_history
                        (event_id, category, price, sold_out, change_date, change_type)
                    VALUES
                        (%(eid)s, %(cat)s, %(price)s, %(sold_out)s, %(now)s, 'REMOVED')
                    """,
                    {
                        "eid": event_id, "cat": cat,
                        "price": rec["price"], "sold_out": rec["sold_out"], "now": now
                    }
                )

    # Bağlam yöneticisi commit / rollback / close işlemlerini otomatik yapar.
    print(f"[{now:%Y-%m-%d %H:%M:%S}] «{event['name']}» işlendi.")

# --------------------------------------------------------------------------- #
# 4. Çalıştırıcı
# --------------------------------------------------------------------------- #
def main():
    print("Bugece verileri çekiliyor…")
    for raw in fetch_events():
        try:
            upsert_event_with_history(normalize_event(raw))
        except Exception as exc:
            # Bir etkinlik hata verse bile akış devam etsin.
            print("⚠️  Hata:", exc)

if __name__ == "__main__":
    main()
