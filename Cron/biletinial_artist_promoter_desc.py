#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Biletinial müzik sayfalarını tarar, etkinlikleri normalize eder ve
upsert_event_with_history() aracılığıyla Supabase/PostgreSQL’e yazar.
Bu betik, Bugece ile aynı price-history mantığını paylaşır.
"""

import os
import time
import random
import json
import html
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Union

import psycopg2
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from decimal import Decimal, InvalidOperation
import re

# ------------------------------------------------------------- #
# 0. Şema SQL dosyasını oku
#    (biletinial_events tablosuna 'promoter', 'artist' ve 'description' sütunlarının eklendiğini varsayıyoruz)
# ------------------------------------------------------------- #

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
# 2. Yardımcı: '₺1.500,00'  ->  Decimal('1500.00') 
# ------------------------------------------------------------- #
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


# ------------------------------------------------------------- #
# 3. HTML → Ham etkinlik veri yapısı (extract_events_from_html)
# ------------------------------------------------------------- #
def extract_events_from_html(html_content: str) -> List[Dict]:
    soup = BeautifulSoup(html_content, "html.parser")
    events: List[Dict] = []

    # -------------------------------------------------------------------
    # 1) Sayfanın en üstünde “Sanatçılar” bilgisi tek seferlik:
    artist = ""
    person_div = soup.find("div", class_="yds_cinema_details_person")
    if person_div:
        artist_links = [a.get_text(strip=True) for a in person_div.find_all("a")]
        artist = ", ".join(artist_links).strip()

    # -------------------------------------------------------------------
    # 2) Sayfanın ortasında “Açıklama (description)” metni:
    description = ""
    # <div class="yds_cinema_movie_thread_info"> altındaki <p> etiketlerini alalım:
    desc_info = soup.find("div", class_="yds_cinema_movie_thread_info")
    if desc_info:
        # Birden fazla <p> olabilir, hepsini birleştirebiliriz:
        paras = [p.get_text(strip=True) for p in desc_info.find_all("p")]
        description = " ".join(paras).strip()

    # -------------------------------------------------------------------
    # 3) Şehir/Seans parçalarını dolaşıyoruz:
    #    Her city_box içinde birden fazla session olabilir.
    for city_box in soup.select("div.ed-biletler__sehir"):
        city = city_box.get("data-sehir", "").strip()

        # Etkinlik adı (sayfanın üstündeki <h1>):
        name_container = soup.find('div', class_='yds_cinema_details_info_title')
        name_tag = name_container.find('h1') if name_container else None
        name = name_tag.get_text(strip=True) if name_tag else 'Unknown'

        for session in city_box.select("div.ed-biletler__sehir__gun"):
            # Mekan (venue):
            loc_tag  = session.find("address", itemprop="name")
            venue    = loc_tag.get_text(strip=True) if loc_tag else "Unknown"

            # Tarih (ISO):
            time_tag = session.find("time", itemprop="startDate")
            date_iso = time_tag.get("content", "") if time_tag else ""

            # Promoter bilgisini çek (<div class="ed-biletler__sehir__gun__organizator"> içinde <span>…</span>):
            promoter = ""
            org_div  = session.find("div", class_="ed-biletler__sehir__gun__organizator")
            if org_div:
                span_tag = org_div.find("span")
                promoter = span_tag.get_text(strip=True) if span_tag else ""

            # Bilet kategorileri & fiyatlar:
            price_list = []
            prices_link = session.find("a", class_="ticket_price_tooltip")
            if prices_link:
                try:
                    decoded = html.unescape(prices_link["data-ticketprices"])
                    price_json = json.loads(decoded)
                    for p in price_json.get("prices", []):
                        price_val = parse_price(p.get("price"))
                        if price_val is None:
                            continue

                        price_list.append({
                            "category": p.get("name", "").strip(),
                            "price": price_val,
                            "sold_out": False,
                        })
                except Exception as err:
                    print("⚠️  price decode:", err)

            # -------------------------------------------------------------------
            # 4) Event objesini oluştururken “artist”, “promoter” ve “description” alanlarını da ekliyoruz:
            events.append({
                "name":         name,
                "venue":        f"{city} {venue}" if city else venue,
                "date":         date_iso,
                "artist":       artist,         # Sayfanın en üstünden çekilen sanatçı(lar)
                "promoter":     promoter,       # Her seans için satırdaki organizatör
                "description":  description,    # Sayfanın ortasındaki açıklama metni
                "price_list":   price_list,
            })

    return events


# ------------------------------------------------------------- #
# 4. Normalizasyon (normalize_biletinial_event)
# ------------------------------------------------------------- #
def normalize_biletinial_event(raw: Dict) -> Dict:
    return {
        "provider":     "Biletinial",
        "name":         raw["name"],
        "venue":        raw["venue"],
        "date":         raw["date"],          
        "artist":       raw.get("artist", ""),       # SANATÇI
        "promoter":     raw.get("promoter", ""),     # ORGANİZATÖR
        "description":  raw.get("description", ""),  # AÇIKLAMA
        "price_list":   raw["price_list"],
    }


# ------------------------------------------------------------- #
# 5. Veritabanı şemasını kontrol et / oluştur 
# ------------------------------------------------------------- #



# ------------------------------------------------------------- #
# 6. Şehir liste sayfasından konser detay linklerini çıkart 
# ------------------------------------------------------------- #
def extract_links_from_city_listing(city_slug: str) -> List[str]:
    """
    https://biletinial.com/tr-tr/muzik/<city_slug> sayfasından
    konser detay linklerini (…/tr-tr/muzik/<city_slug>/<etkinlik-slug>) döndürür.
    """
    url = f"https://biletinial.com/tr-tr/muzik/{city_slug}"
    html_page = requests.get(url, headers=HEADERS, timeout=15).text
    base = "https://biletinial.com"
    links = [
        base + a["href"]
        for a in BeautifulSoup(html_page, "html.parser").find_all("a", href=True)
        if a["href"].startswith("/tr-tr/muzik/")
    ]
    return sorted(set(links))   # Tekilleştir ve sıralı döndür


# ------------------------------------------------------------- #
# 7. Belirli bir şehir için tüm etkinlikleri çek (HTML → ham event list)
# ------------------------------------------------------------- #
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

        time.sleep(random.uniform(1, 3))  # Kibar ol: 1–3 saniye rastgele bekle
    return events


# ------------------------------------------------------------- #
# 8. Upsert mantığı: Etkinlik + fiyat geçmişi
# ------------------------------------------------------------- #
def format_pg_array(value: Optional[str]) -> Optional[str]:
    """
    PostgreSQL array formatına uygun string döndürür: {"value"}
    Eğer value boş veya None ise None döndürür.
    Eğer zaten {…} ile başlıyorsa dokunmaz.
    """
    if not value:
        return None
    if value.startswith("{") and value.endswith("}"):
        return value
    escaped = value.replace('"', '\\"')
    return f'{{"{escaped}"}}'


def upsert_event_with_history(event: dict) -> None:
    """
    * Etkinlik var mı? Yoksa ekle, varsa güncelle.
    * Aktif fiyatlar ile gelen listeyi karşılaştır:
        - Değişenler  → history'ye 'UPDATED', fiyatları güncelle.
        - Yeniler      → prices & history'ye 'ADDED'.
        - Silinenler   → prices.is_active = FALSE, history'ye 'REMOVED'.
    """
    now = datetime.now()

    with connect_db() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:

        # ---- 8.1 Etkinlik upsert (artist + promoter + description) ------------
        cur.execute(
            """
            SELECT id
            FROM biletinial_events
            WHERE name = %(name)s
              AND venue = %(venue)s
              AND date = %(date)s
            """,
            {
                "name":  event["name"],
                "venue": event["venue"],
                "date":  event["date"]
            }
        )
        row = cur.fetchone()

        if row:
            # Mevcut etkinlik → UPDATE
            event_id = row["id"]
            cur.execute(
                """
                UPDATE biletinial_events
                SET provider     = %(provider)s,
                    artist       = %(artist)s,
                    promoter     = %(promoter)s,
                    description  = %(description)s,
                    last_seen    = %(now)s
                WHERE id = %(id)s
                """,
                {
                    "provider":    event["provider"],
                    "artist":      format_pg_array(event.get("artist", "")),
                    "promoter":    format_pg_array(event.get("promoter", "")),
                    "description": event.get("description", ""),
                    "now":         now,
                    "id":          event_id
                }
            )
        else:
            # Yeni etkinlik → INSERT
            cur.execute(
                """
                INSERT INTO biletinial_events
                    (provider, name, venue, date, artist, promoter, description, created_at, last_seen)
                VALUES
                    (%(provider)s, %(name)s, %(venue)s, %(date)s,
                     %(artist)s, %(promoter)s, %(description)s, %(now)s, %(now)s)
                RETURNING id
                """,
                {
                    "provider":    event["provider"],
                    "name":        event["name"],
                    "venue":       event["venue"],
                    "date":        event["date"],
                    "artist":      format_pg_array(event.get("artist", "")),
                    "promoter":    format_pg_array(event.get("promoter", "")),
                    "description": event.get("description", ""),
                    "now":         now
                }
            )
            event_id = cur.fetchone()["id"]

        # ---- 8.2 Mevcut aktif fiyatları al -----------------------------------
        cur.execute(
            """
            SELECT id, category, price, sold_out
            FROM biletinial_prices
            WHERE event_id = %(eid)s
              AND is_active = TRUE
            """,
            {"eid": event_id}
        )
        existing = {r["category"]: r for r in cur.fetchall()}
        seen_categories = set()

        # ---- 8.3 Gelen price_list ile eşleştir: değişen/yenileri ekle ----------
        for p in event["price_list"]:
            cat, price_val, sold_out = p["category"], p["price"], p["sold_out"]
            seen_categories.add(cat)

            if cat in existing:
                prev = existing[cat]
                if (prev["price"], prev["sold_out"]) != (price_val, sold_out):
                    # Fiyat veya sold_out değişmiş → history kaydına 'UPDATED'
                    cur.execute(
                        """
                        INSERT INTO biletinial_price_history
                            (event_id, category, price, sold_out, change_date, change_type)
                        VALUES
                            (%(eid)s, %(cat)s, %(old_price)s, %(old_so)s, %(now)s, 'UPDATED')
                        """,
                        {
                            "eid":       event_id,
                            "cat":       cat,
                            "old_price": prev["price"],
                            "old_so":    prev["sold_out"],
                            "now":       now
                        }
                    )
                    # Fiyat tablosunu güncelle
                    cur.execute(
                        """
                        UPDATE biletinial_prices
                        SET price      = %(price)s,
                            sold_out   = %(sold_out)s,
                            last_seen  = %(now)s
                        WHERE id = %(id)s
                        """,
                        {
                            "price":     price_val,
                            "sold_out":  sold_out,
                            "now":       now,
                            "id":        prev["id"]
                        }
                    )
            else:
                # Yeni kategori → biletinial_prices ve history'ye 'ADDED'
                cur.execute(
                    """
                    INSERT INTO biletinial_prices
                        (event_id, category, price, sold_out, created_at, last_seen, is_active)
                    VALUES
                        (%(eid)s, %(cat)s, %(price)s, %(sold_out)s, %(now)s, %(now)s, TRUE)
                    RETURNING id
                    """,
                    {
                        "eid":       event_id,
                        "cat":       cat,
                        "price":     price_val,
                        "sold_out":  sold_out,
                        "now":       now
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
                        "eid":       event_id,
                        "cat":       cat,
                        "price":     price_val,
                        "sold_out":  sold_out,
                        "now":       now
                    }
                )

        # ---- 8.4 Listede olmayan kategorileri pasifleştir + history 'REMOVED' ---
        for cat, rec in existing.items():
            if cat not in seen_categories:
                cur.execute(
                    """
                    UPDATE biletinial_prices
                    SET is_active = FALSE,
                        last_seen = %(now)s
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
                        "eid":       event_id,
                        "cat":       cat,
                        "price":     rec["price"],
                        "sold_out":  rec["sold_out"],
                        "now":       now
                    }
                )

    # Bağlam yöneticisi commit/rollback işlemlerini otomatik yapar.
    print(f"[{now:%Y-%m-%d %H:%M:%S}] «{event['name']}» işlendi.")


# ------------------------------------------------------------- #
# 9. Orkestrasyon: Tüm şehirler için scrape işlemini başlat
# ------------------------------------------------------------- #
CITIES = [
    "istanbul-avrupa",
    "istanbul-anadolu",
    "ankara",
    "izmir",
]

def scrape_biletinial_events():
    total = 0
    for city in CITIES:
        print(f"\n=== {city} ===")
        for raw_event in fetch_city_events(city):
            try:
                normed = normalize_biletinial_event(raw_event)
                upsert_event_with_history(normed)
                total += 1
            except Exception as exc:
                print("⚠️  DB hata:", exc)
    print(f"\n{total} etkinlik işlendi.")


# ------------------------------------------------------------- #
# 10. Ana bloğu: Şema kontrolü yap ve scrape işlemini başlat
# ------------------------------------------------------------- #
if __name__ == "__main__":
    scrape_biletinial_events()
