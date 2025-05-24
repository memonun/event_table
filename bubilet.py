import requests
import pandas as pd
from datetime import datetime
import os

def get_all_events():
    url = "https://apiv2.bubilet.com.tr/api/Anasayfa/6/Etkinlikler"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def get_ticket_details(seans_id):
    url = f"https://apiv2.bubilet.com.tr/api/Seans/{seans_id}/Biletler"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def scrape_istanbul_events():
    all_data = []
    events = get_all_events()
    print(f"🔍 {len(events)} etkinlik bulundu (İstanbul)")

    for event in events:
        event_name = event.get("etkinlikAdi")
        seanslar = event.get("seanslar", [])

        for seans in seanslar:
            seans_id = seans.get("seansId")
            seans_gizli = seans.get("seansGizle")

            if not seans_id or seans_gizli:
                print(f"⚠️ Geçersiz seansID: {event_name}")
                continue

            try:
                ticket_info = get_ticket_details(seans_id)
                venue = ticket_info.get("mekanAdi")
                categories = ticket_info.get("seansBiletler", [])

                if not categories:
                    print(f"⚠️ {event_name} için kategori bulunamadı (SeansID: {seans_id})")
                    continue

                for cat in categories:
                    all_data.append({
                        "event_name": event_name,
                        "venue_name": venue,
                        "event_date": cat.get("tarih"),
                        "category_name": cat.get("biletKategoriAdi"),
                        "price": cat.get("fiyat"),
                        "remaining_tickets": cat.get("kalanBilet"),
                        "ticket_active": cat.get("biletAktif"),
                        "seansID": seans_id,
                        "scrape_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })

                print(f"✅ {event_name} (SeansID: {seans_id}) işlendi")

            except Exception as e:
                print(f"❌ Hata - SeansID {seans_id}: {e}")

    return all_data

if __name__ == "__main__":
    result = scrape_istanbul_events()

    if result:
        df = pd.DataFrame(result)
        file_path = "data/bubilet_istanbul_data.csv"
        write_header = not os.path.exists(file_path)
        df.to_csv(file_path, mode='a', header=write_header, index=False)
        print("\n✅ CSV'ye veri eklendi → data/bubilet_istanbul_data.csv")
    else:
        print("\n⚠️ Veri çekilemedi, CSV oluşturulmadı.")