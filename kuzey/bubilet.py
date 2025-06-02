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
        # Her çalıştırmada benzersiz bir dosya adı oluştur (sıralı)
        folder = "data"
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)

        # Var olan dosyaları bul ve en yüksek numarayı tespit et
        import re
        existing_files = [f for f in os.listdir(folder) if re.match(r"bubilet_istanbul_data(_\d+)?\.csv$", f)]
        max_index = 0
        for fname in existing_files:
            match = re.match(r"bubilet_istanbul_data_(\d+)\.csv$", fname)
            if match:
                idx = int(match.group(1))
                if idx > max_index:
                    max_index = idx
            elif fname == "bubilet_istanbul_data.csv":
                if max_index == 0:
                    max_index = 1  # Eğer sadece ana dosya varsa, bir sonrakine 2 ver

        # Yeni dosya adını belirle
        if max_index == 0:
            file_path = os.path.join(folder, "bubilet_istanbul_data_1.csv")
        else:
            file_path = os.path.join(folder, f"bubilet_istanbul_data_{max_index+1}.csv")

        df.to_csv(file_path, index=False)
        print(f"\n✅ CSV'ye veri eklendi → {file_path}")
    else:
        print("\n⚠️ Veri çekilemedi, CSV oluşturulmadı.")