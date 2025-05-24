import requests
import pandas as pd
import time

# Base API endpoints
PROMOTERS_API = "https://barac.bugece.co/v1/promoters"
EVENTS_API_TEMPLATE = "https://barac.bugece.co/v1/event/list?country=298795&promoter={slug}"

# Request headers
HEADERS = {
    "Accept": "application/json"
}

# CSV'ye kaydedilecek etkinlik verisi
all_events = []

def fetch_all_promoters():
    """Tüm promoter'ları API'den sayfa sayfa çek"""
    promoters = []
    page = 1

    while True:
        print(f"[INFO] Fetching promoters - Page {page}")
        url = f"{PROMOTERS_API}?countryId=298795&pageSize=24&page={page}"
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            data = response.json().get("data", {})
            items = data.get("items", [])

            if not items:
                break

            for item in items:
                promoters.append({
                    "name": item.get("name"),
                    "slug": item.get("slug"),
                    "website": item.get("website"),
                    "social_media": item.get("social_media"),
                    "desc": item.get("desc"),
                    "isActive": item.get("isActive"),
                    "short_url": item.get("short_url")
                })

            if page >= data.get("totalPage", 1):
                break
            page += 1
            time.sleep(0.5)  # API'yi yavaşlatmamak için

        except Exception as e:
            print(f"[ERROR] Failed to fetch promoters on page {page}: {e}")
            break

    return promoters

def fetch_events_for_promoter(promoter):
    """Verilen bir promoter için etkinlikleri çek"""
    slug = promoter.get("slug")
    name = promoter.get("name")

    if not slug:
        print(f"[WARNING] Promoter '{name}' has no slug. Skipping.")
        return []

    url = EVENTS_API_TEMPLATE.format(slug=slug)
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        items = response.json().get("data", {}).get("items", [])
        event_list = []

        for event in items:
            event_list.append({
                "promoter": name,
                "event_title": event.get("name"),
                "event_date": event.get("date"),
                "venue_name": event.get("venue", {}).get("name"),
                "timestamp": event.get("start_time")
            })

        return event_list

    except Exception as e:
        print(f"[ERROR] Failed to fetch events for promoter '{slug}': {e}")
        return []

def main():
    print("[START] Fetching all Bugece promoters and their events...")

    # 1. Promoter'ları al
    promoters = fetch_all_promoters()

    # 2. Her promoter için etkinlikleri al
    for promoter in promoters:
        events = fetch_events_for_promoter(promoter)
        all_events.extend(events)
        time.sleep(0.3)

    # 3. DataFrame oluştur ve CSV olarak kaydet
    df = pd.DataFrame(all_events)
    df.to_csv("bugece_events.csv", index=False)
    print(f"[DONE] {len(df)} events saved to 'bugece_events.csv'.")

if __name__ == "__main__":
    main()