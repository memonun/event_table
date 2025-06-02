import requests
import pandas as pd
import os
from datetime import datetime
from tqdm import tqdm
from psycopg2.extras import execute_values
import psycopg2
from dotenv import load_dotenv
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL, sslmode="require")

# --------------------------- #
# API'den verileri çek
# --------------------------- #
def fetch_all_events():
    url = "https://apiv2.bubilet.com.tr/api/Anasayfa/2/Etkinlikler"
    response = requests.get(url)
    return response.json()

def fetch_ticket_details(seans_id):
    url = f"https://apiv2.bubilet.com.tr/api/Seans/{seans_id}/Biletler"
    response = requests.get(url)
    if response.status_code != 200:
        return None
    return response.json()

def fetch_artist_name(event_id):
    url = f"https://apiv2.bubilet.com.tr/api/v2/event/{event_id}/performer"
    response = requests.get(url)
    try:
        return response.json()["data"]["list"][0].get("adiSoyadi")
    except:
        return None

# --------------------------- #
# Supabase'a upsert işlemi
# --------------------------- #
def upsert_event_with_history(conn, event):
    with conn.cursor() as cur:
        # --- upsert bubilet_events in the new format ---
        cur.execute("""
            INSERT INTO bubilet_events (
                id, provider, name, venue, date, genre,
                created_at, last_seen, canonical_venue_id,
                description, promoter, artist
            )
            VALUES (
                %(id)s, %(provider)s, %(name)s, %(venue)s, %(date)s, %(genre)s,
                %(created_at)s, %(last_seen)s, %(canonical_venue_id)s,
                %(description)s, %(promoter)s, %(artist)s
            )
            ON CONFLICT (id) DO UPDATE SET
                provider            = EXCLUDED.provider,
                name                = EXCLUDED.name,
                venue               = EXCLUDED.venue,
                date                = EXCLUDED.date,
                genre               = EXCLUDED.genre,
                last_seen           = EXCLUDED.last_seen,
                canonical_venue_id  = EXCLUDED.canonical_venue_id,
                description         = EXCLUDED.description,
                promoter            = EXCLUDED.promoter,
                artist              = EXCLUDED.artist;
        """, event)

        cur.execute("SELECT * FROM bubilet_prices WHERE event_id = %(id)s", {"id": event["id"]})
        existing_prices = cur.fetchall()
        existing_price_keys = {(row[0], row[1], row[7]) for row in existing_prices}  # (event_id, category, is_active)

        new_prices = []
        history_rows = []

        for p in event["price_list"]:
            key = (event["id"], p["category"], p["is_active"])
            if key not in existing_price_keys:
                change_type = "ADDED"
            else:
                # Find existing price
                existing = next((row for row in existing_prices if row[0] == event["id"] and row[1] == p["category"] and row[7] == p["is_active"]), None)
                if existing and (existing[2] != p["price"] or existing[3] != p["remaining"]):
                    change_type = "UPTADED"
                else:
                    change_type = None

            new_prices.append({
                "event_id": event["id"],
                "category": p["category"],
                "price": p["price"],
                "remaining": p["remaining"],
                "sold_out": p["sold_out"],
                "created_at": p["created_at"],
                "last_seen": p["last_seen"],
                "is_active": p["is_active"]
            })

            if change_type:
                history_rows.append({
                    "event_id": event["id"],
                    "category": p["category"],
                    "price": p["price"],
                    "remaining": p["remaining"],
                    "sold_out": p["sold_out"],
                    "change_date": p["last_seen"],
                    "change_type": change_type
                })

        unique_keys = set()
        filtered_prices = []
        for p in new_prices:
            key = (p["event_id"], p["category"], p["is_active"])
            if key not in unique_keys:
                filtered_prices.append(p)
                unique_keys.add(key)

        execute_values(cur,
            """INSERT INTO bubilet_prices
               (event_id, category, price, remaining, sold_out,
                created_at, last_seen, is_active)
               VALUES %s
               ON CONFLICT (event_id, category, is_active)
               DO UPDATE SET price = EXCLUDED.price,
                             remaining = EXCLUDED.remaining,
                             last_seen = EXCLUDED.last_seen""",
            [(
                p["event_id"], p["category"], p["price"], p["remaining"],
                p["sold_out"], p["created_at"], p["last_seen"], p["is_active"]
            ) for p in filtered_prices])

        if history_rows:
            execute_values(cur,
                """INSERT INTO bubilet_price_history
                   (event_id, category, price, remaining, sold_out,
                    change_date, change_type)
                   VALUES %s""",
                [(
                    h["event_id"], h["category"], h["price"], h["remaining"],
                    h["sold_out"], h["change_date"], h["change_type"]
                ) for h in history_rows])

    conn.commit()

# --------------------------- #
# Tarih damgası
# --------------------------- #
now = datetime.now().isoformat()

# --------------------------- #
# Verileri topla
# --------------------------- #

# --------------------------- #
# Etkinlikleri işle
# --------------------------- #
events = fetch_all_events()

for event in tqdm(events, desc="Etkinlikler işleniyor"):
    etkinlikAdi = event.get("etkinlikAdi")
    etkinlikId = event.get("etkinlikId")
    seanslar = event.get("seanslar", [])

    artist_name = fetch_artist_name(etkinlikId)

    for seans in seanslar:
        seansId = seans.get("seansId")
        tarih = seans.get("tarih")
        detail = fetch_ticket_details(seansId)
        if not detail:
            continue

        venue = detail.get("mekanAdi")
        city = detail.get("ilAdi")

        price_list = []

        for bilet in detail.get("seansBiletler", []):
            category = bilet.get("biletKategoriAdi")
            price = float(bilet.get("fiyat", 0))
            remaining = int(bilet.get("kalanBilet", 0))
            sold_out = remaining == 0
            is_active = bilet.get("biletAktif", False)

            price_list.append({
                "category": category,
                "price": price,
                "remaining": remaining,
                "sold_out": sold_out,
                "created_at": now,
                "last_seen": now,
                "is_active": is_active
            })

        event_dict = {
            "id": seansId,
            "provider": "Bubilet",
            "name": etkinlikAdi,
            "venue": venue,
            "date": tarih,
            "genre": None,
            "created_at": datetime.now(),
            "last_seen": datetime.now(),
            "canonical_venue_id": None,
            "description": None,
            "promoter": None,
            "artist": [artist_name] if artist_name else None,
            "price_list": price_list
        }

        upsert_event_with_history(conn, event_dict)

conn.close()
print("✅ Bubilet verileri Supabase’e aktarıldı.")