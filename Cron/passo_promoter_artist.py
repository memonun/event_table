import os
import re

import openpyxl
import psycopg2
import requests
from dotenv import load_dotenv
from openpyxl import Workbook

from datetime import datetime
from psycopg2.extras import RealDictCursor

load_dotenv()  # .env içinden DATABASE_URL al
DATABASE_URL = os.getenv("DATABASE_URL")

def connect_db():
    """Supabase TLS gerektirdiği için sslmode='require' parametresi ile bağlan."""
    return psycopg2.connect(DATABASE_URL, sslmode="require")


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

        # ---- 3.1 Etkinlik upsert (promoter + artist ile) --------------------------
        cur.execute(
            """
            SELECT id
            FROM passo_events
            WHERE name = %(name)s
              AND venue = %(venue)s
              AND date = %(date)s
            """,
            {
                "name": event["name"],
                "venue": event["venue"],
                "date": event["date"]
            }
        )
        row = cur.fetchone()

        if row:
            # Mevcut etkinlik → UPDATE
            event_id = row["id"]
            cur.execute(
                """
                UPDATE passo_events
                SET provider    = %(provider)s,
                    description = %(description)s,
                    genre       = %(genre)s,
                    promoter    = %(promoter)s,
                    artist      = %(artist)s,
                    last_seen   = %(now)s
                WHERE id = %(id)s
                """,
                {
                    "provider": event["provider"],
                    "description": event["description"],
                    "genre": event["genre"],
                    "promoter": event.get("promoter"),
                    "artist": event.get("artist"),
                    "now": now,
                    "id": event_id
                }
            )
        else:
            # Yeni etkinlik → INSERT
            cur.execute(
                """
                INSERT INTO passo_events
                    (provider, name, description, venue, date, genre, promoter, artist, created_at, last_seen)
                VALUES
                    (%(provider)s, %(name)s, %(description)s, %(venue)s, %(date)s,
                     %(genre)s, %(promoter)s, %(artist)s, %(now)s, %(now)s)
                RETURNING id
                """,
                {
                    "provider": event["provider"],
                    "name": event["name"],
                    "description": event["description"],
                    "venue": event["venue"],
                    "date": event["date"],
                    "genre": event["genre"],
                    "promoter": event.get("promoter"),
                    "artist": event.get("artist"),
                    "now": now
                }
            )
            event_id = cur.fetchone()["id"]

        # ---- 3.2 Aktif fiyatları çek ------------------------------------------------
        cur.execute(
            """
            SELECT id, category, price, sold_out
            FROM passo_prices
            WHERE event_id = %(eid)s
              AND is_active = TRUE
            """,
            {"eid": event_id}
        )
        existing = {r["category"]: r for r in cur.fetchall()}
        seen_categories = set()

        # ---- 3.3 Yeni gelen listeyi işle -------------------------------------------
        for p in event["price_list"]:
            cat, price_val, sold_out = p["category"], p["price"], p["sold_out"]
            seen_categories.add(cat)

            if cat in existing:
                prev = existing[cat]
                if (prev["price"], prev["sold_out"]) != (price_val, sold_out):
                    # Fiyat veya sold_out değişmiş → history ekle + güncelle
                    cur.execute(
                        """
                        INSERT INTO passo_price_history
                            (event_id, category, price, sold_out, change_date, change_type)
                        VALUES
                            (%(eid)s, %(cat)s, %(old_price)s, %(old_so)s, %(now)s, 'UPDATED')
                        """,
                        {
                            "eid": event_id,
                            "cat": cat,
                            "old_price": prev["price"],
                            "old_so": prev["sold_out"],
                            "now": now
                        }
                    )
                    cur.execute(
                        """
                        UPDATE passo_prices
                        SET price      = %(price)s,
                            sold_out   = %(sold_out)s,
                            last_seen  = %(now)s
                        WHERE id = %(id)s
                        """,
                        {
                            "price": price_val,
                            "sold_out": sold_out,
                            "now": now,
                            "id": prev["id"]
                        }
                    )
            else:
                # Yeni kategori → passo_prices ve history'ye ekle
                cur.execute(
                    """
                    INSERT INTO passo_prices
                        (event_id, category, price, sold_out, created_at, last_seen, is_active)
                    VALUES
                        (%(eid)s, %(cat)s, %(price)s, %(sold_out)s, %(now)s, %(now)s, TRUE)
                    RETURNING id
                    """,
                    {
                        "eid": event_id,
                        "cat": cat,
                        "price": price_val,
                        "sold_out": sold_out,
                        "now": now
                    }
                )
                cur.execute(
                    """
                    INSERT INTO passo_price_history
                        (event_id, category, price, sold_out, change_date, change_type)
                    VALUES
                        (%(eid)s, %(cat)s, %(price)s, %(sold_out)s, %(now)s, 'ADDED')
                    """,
                    {
                        "eid": event_id,
                        "cat": cat,
                        "price": price_val,
                        "sold_out": sold_out,
                        "now": now
                    }
                )

        # ---- 3.4 Artık listede olmayan kategorileri pasifleştir ---------------------
        for cat, rec in existing.items():
            if cat not in seen_categories:
                cur.execute(
                    """
                    UPDATE passo_prices
                    SET is_active = FALSE,
                        last_seen = %(now)s
                    WHERE id = %(id)s
                    """,
                    {"now": now, "id": rec["id"]}
                )
                cur.execute(
                    """
                    INSERT INTO passo_price_history
                        (event_id, category, price, sold_out, change_date, change_type)
                    VALUES
                        (%(eid)s, %(cat)s, %(price)s, %(sold_out)s, %(now)s, 'REMOVED')
                    """,
                    {
                        "eid": event_id,
                        "cat": cat,
                        "price": rec["price"],
                        "sold_out": rec["sold_out"],
                        "now": now
                    }
                )

    # Bağlam yöneticisi commit/rollback işlemlerini otomatik yapar.
    print(f"[{now:%Y-%m-%d %H:%M:%S}] «{event['name']}» işlendi.")


session = requests.Session()

url = "https://ticketingweb.passo.com.tr/api/passoweb/allevents"

headers = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Referer": "https://www.passo.com.tr/",
    "Origin": "https://www.passo.com.tr",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
}

categories_and_ids = {
    "music": 8615,
    "performing_arts": 11615,
    "sports": 13615,
    "museum": 15615,
    "football": 4615,
    "other": 12615,
}

all_events = []
start = 0
size = 100  # Adjust based on API limits

payload = {"GenreId": "8615", "LanguageId": 618, "from": 0, "size": 1000}
response = session.post(url, headers=headers, json=payload)

print(response)

if response.status_code == 200:
    doc = response.json()
    events = doc.get("valueList", [])
    print(f"Found {len(events)} events")

    for event in events:
        try:
            seo_url = event["seoUrl"]
            event_id = event["id"]

            event_details_url = f"https://ticketingweb.passo.com.tr/api/passoweb/geteventdetails/{seo_url}/{event_id}/618"

            # Etkinlik detaylarını alıyoruz
            event_detail_response = session.get(event_details_url, headers=headers)

            if event_detail_response.status_code != 200:
                print(f"Failed to get details for event {event_id}. Status code: {event_detail_response.status_code}")
                continue

            if not event_detail_response.content:
                print(f"Empty response for event {event_id}")
                continue

            try:
                event_detail_json = event_detail_response.json()
            except requests.exceptions.JSONDecodeError:
                print(
                    f"Non-JSON response received for event {event_id}. Content: {event_detail_response.text[:100]}..."
                )
                continue

            value = event_detail_json.get("value", {})

            # 1) organizerName
            organizer_name = value.get("organizerName", None)
            # 2) detail içindeki name → artist olarak kaydedilecek
            artist_name = value.get("name", None)

            artist_list = [artist_name] if artist_name else None

            genre = value.get("genreName", None)
            sub_category = value.get("subGenreName", None)
            price_list_raw = value.get("categories", [])

            all_tickets = []
            for ticket in price_list_raw:
                name = ticket.get("name", "")
                price = ticket.get("price", 0)

                sold_out = "TÜKENDİ" in name.upper()
                clean_name = re.sub(r"[\s\-\(\[]*TÜKENDİ[\s\-\)\]]*", "", name, flags=re.IGNORECASE).strip()

                all_tickets.append({
                    "category": clean_name,
                    "price": price,
                    "sold_out": sold_out
                })

            current_event = {
                "provider": "Passo",
                "name": event["name"],
                "description": event["seoDescription"],
                "venue": event["venueName"],
                "date": event["date"],
                "genre": sub_category,
                "promoter": organizer_name,    # Eski hali: organizerName → promoter
                "artist": artist_list,         # Yeni eklenen satır: detail içindeki name → artist
                "price_list": all_tickets
            }

            upsert_event_with_history(current_event)
            print(current_event)

        except Exception as e:
            print(f"Error processing event: {str(e)}")
            continue

print(f"Total events processed: {len(all_events)}")
