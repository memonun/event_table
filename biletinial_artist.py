#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Biletinial müzik sayfalarını tarar, etkinlikleri normalize eder ve
upsert_event_with_history() aracılığıyla Supabase/PostgreSQL’e yazar.
Bu betik, Bugece ile aynı price-history mantığını paylaşır.
"""

import os, time, random, json, html
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import psycopg2
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

SCHEMA_SQL = Path("schema.sql").read_text(encoding="utf-8")



# ------------------------------------------------------------- #
# 1. Ortak araçlar
# ------------------------------------------------------------- #
load_dotenv()                                   # .env içinden DATABASE_URL al
DATABASE_URL = os.getenv("DATABASE_URL")

def connect_db():
    """Supabase TLS gerektirdiği için sslmode='require' parametresi ile bağlan."""
    return psycopg2.connect(DATABASE_URL, sslmode="require")

HEADERS = {"User-Agent": "Mozilla/5.0"}

# ------------------------------------------------------------- #
# 2. HTML → Ham etkinlik veri yapısı
# ------------------------------------------------------------- #
def extract_events_from_html(html_content: str) -> List[Dict]:
    soup = BeautifulSoup(html_content, "html.parser")
    events: List[Dict] = []

    # Her film/konser detay sayfasında (ör. …/muzik/istanbul-avrupa/…)
    #   <div class="ed-biletler__sehir">  → şehir / seans grupları
    for city_box in soup.select("div.ed-biletler__sehir"):
        city = city_box.get("data-sehir", "").strip()


        name_container = soup.find('div', class_='yds_cinema_details_info_title')
        name_tag = name_container.find('h1') if name_container else None
        name = name_tag.get_text(strip=True) if name_tag else 'Unknown'

        for session in city_box.select("div.ed-biletler__sehir__gun"):
            # ---------------- Öznitelikler ----------------
            loc_tag  = session.find("address", itemprop="name")
            venue    = loc_tag.get_text(strip=True) if loc_tag else "Unknown"

            time_tag = session.find("time", itemprop="startDate")
            date_iso = time_tag.get("content", "") if time_tag else ""

            org_div  = session.find("div", class_="ed-biletler__sehir__gun_organizator")
            organizer= (org_div.span.get_text(strip=True)
                        if org_div and org_div.span else "")

            # -------------- Bilet kategorileri -------------
            price_list = []
            prices_link = session.find("a", class_="ticket_price_tooltip")
            if prices_link:
                try:
                    decoded = html.unescape(prices_link["data-ticketprices"])
                    price_json = json.loads(decoded)
                    for p in price_json.get("prices", []):
                        price_val = parse_price(p.get("price"))

                        if price_val is None:
                            continue  # geçersiz fiyat satırını atla
                            # veya: price_val = Decimal("0")

                        price_list.append(
                            {
                                "category": p.get("name", "").strip(),
                                "price": price_val,  # Decimal nesnesi → DB’de NUMERIC
                                "sold_out": False,
                            }
                        )
                except Exception as err:
                    print("⚠️  price decode:", err)

            events.append(
                {
                    "name":        name,
                    "venue":       f"{city} {venue}" if city else venue,
                    "date":        date_iso,
                    "genre":       organizer or "",
                    "price_list":  price_list,
                }
            )
    return events

# ------------------------------------------------------------- #
# 3. Normalizasyon (upsert fonksiyonunun beklediği format)
# ------------------------------------------------------------- #

# -------------------------------------------------------------
# Yardımcı: '₺1.500,00'  ->  1500.00   (Decimal döner)
# -------------------------------------------------------------
import re
from decimal import Decimal, InvalidOperation
from typing import Optional, Union

Number = Union[int, float, Decimal]

def parse_price(raw: Union[str, Number, None]) -> Optional[Decimal]:
    """
    Biletinial 'price' alanını güvenle Decimal'a çevirir.
    Kabul edilen örnekler:
        '₺1.500,00', '1.500,00 ₺', '1.500 TL', '650,00', 1500, None
    Dönüş: Decimal veya None
    """
    if raw is None:
        return None

    # Zaten nümerik mi?
    if isinstance(raw, (int, float, Decimal)):
        return Decimal(str(raw))

    # --- Metin temizleme ----------------------------------------------------
    txt = str(raw).strip()

    # “Ücretsiz” vb. durumlar
    if txt.lower() in {"ücretsiz", "free"}:
        return Decimal("0")

    # Para sembolleri / birimleri
    txt = (txt.replace("₺", "")
             .replace("TL", "")
             .replace("tl", "")
             .strip())

    # Binlik ve ondalık ayırıcıları dönüştür
    txt = txt.replace(".", "")   # 1.500,00  →  1500,00
    txt = txt.replace(",", ".")  # 1500,00   →  1500.00

    # Harf kalıntılarını sil (ör. “/KDV dâhil”)
    m = re.search(r"[-+]?\d*\.?\d+", txt)
    if not m:
        return None

    try:
        return Decimal(m.group())
    except InvalidOperation:
        return None

def normalize_biletinial_event(raw: Dict) -> Dict:
    return {
        "provider":   "Biletinial",
        "name":       raw["name"],
        "venue":      raw["venue"],
        "date":       raw["date"],           # ISO (yyyy-MM-ddTHH:mm:ss) geliyor
        "genre":      raw["genre"],
        "price_list": raw["price_list"],
    }

def ensure_schema():
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    print("Şema kontrolü tamamlandı ✔︎")

# ------------------------------------------------------------- #
# 4. Sayfa linklerini bul –> içerikleri indir
# ------------------------------------------------------------- #
def extract_links_from_city_listing(city_slug: str) -> List[str]:
    """
    https://biletinial.com/tr-tr/muzik/<city_slug> sayfasından
    konser detay linklerini (…/muzik/<city>/<event-slug>) döndürür.
    """
    url = f"https://biletinial.com/tr-tr/muzik/{city_slug}"
    html_page = requests.get(url, headers=HEADERS, timeout=15).text
    base = "https://biletinial.com"
    links = [
        base + a["href"]
        for a in BeautifulSoup(html_page, "html.parser").find_all("a", href=True)
        if a["href"].startswith("/tr-tr/muzik/")
    ]
    return sorted(set(links))   # tekilleştir

def fetch_city_events(city_slug: str) -> List[Dict]:
    events: List[Dict] = []
    for link in extract_links_from_city_listing(city_slug):
        try:
            resp = requests.get(link, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                events += extract_events_from_html(resp.text)
            else:
                print(f"⚠️  {link} — HTTP {resp.status_code}")
        except Exception as exc:
            print(f"⚠️  {link} —", exc)

        time.sleep(random.uniform(1, 3))  # kibar ol
    return events

# ------------------------------------------------------------- #
# 5. Orkestrasyon
# ------------------------------------------------------------- #
CITIES = [  # kısaltılmış; tam listeyi isterse ekleyin
    "istanbul-avrupa",
    "istanbul-anadolu",
    "ankara",
    "izmir",
]

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
            SELECT id FROM biletinial_events
            WHERE name = %(name)s AND venue = %(venue)s AND date = %(date)s
            """,
            {"name": event["name"], "venue": event["venue"], "date": event["date"]}
        )
        row = cur.fetchone()

        if row:  # güncelle
            event_id = row["id"]
            cur.execute(
                """
                UPDATE biletinial_events
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
                INSERT INTO biletinial_events
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
            FROM biletinial_prices
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
                        INSERT INTO biletinial_price_history
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
                        UPDATE biletinial_prices
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
                    INSERT INTO biletinial_prices
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
                    INSERT INTO biletinial_price_history
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
                    UPDATE biletinial_prices
                    SET is_active = FALSE, last_seen = %(now)s
                    WHERE id = %(id)s
                    """,
                    {"now": now, "id": rec["id"]}
                )
                cur.execute(
                    """
                    INSERT INTO biletinial_price_history
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

def scrape_biletinial_events():
    total = 0
    for city in CITIES:
        print(f"\n=== {city} ===")
        for raw_event in fetch_city_events(city):
            try:
                upsert_event_with_history(normalize_biletinial_event(raw_event))
                total += 1
            except Exception as exc:
                print("⚠️  DB hata:", exc)
    print(f"\n{total} etkinlik işlendi.")

# ------------------------------------------------------------- #
if __name__ == "__main__":
    ensure_schema()
    scrape_biletinial_events()
