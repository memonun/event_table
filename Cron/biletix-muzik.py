import pg8000
import re
import time
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options



class BiletixInfoLoader:
    def __init__(self, url, max_clicks=40):
        self.url = url
        self.max_clicks = max_clicks
        self.driver = self._setup_driver()

    def _setup_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        return webdriver.Chrome(service=Service(), options=options)

    def load_page(self):
        self.driver.get(self.url)
        time.sleep(random.uniform(2, 5))  # Random wait to avoid bot detection
        self._click_load_more()

    def _click_load_more(self):
        for _ in range(self.max_clicks):
            try:
                button = WebDriverWait(self.driver, random.uniform(2, 5)).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "a.search_load_more"))
                )
                self.driver.execute_script("arguments[0].scrollIntoView();", button)
                time.sleep(random.uniform(1, 3))  # Mimic human-like delay before clicking
                button.click()
                time.sleep(random.uniform(2, 5))  # Allow content to load
            except Exception:
                # break
                continue

    def extract_event_ids(self):
        time.sleep(random.uniform(1, 3))  # Add variation before parsing content
        html_content = self.driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        etkinlik_pattern = re.compile(r"window\.location='/etkinlik/([\w\d]+)/")
        etkinlik_grup_pattern = re.compile(r"window\.location='/etkinlik-grup/(\d+)/")

        etkinlik_list = set()
        etkinlik_grup_list = set()

        for div in soup.find_all('div', onclick=True):
            match_event = etkinlik_pattern.search(div['onclick'])
            match_group = etkinlik_grup_pattern.search(div['onclick'])
            if match_event:
                etkinlik_list.add(match_event.group(1))
            if match_group:
                etkinlik_grup_list.add(match_group.group(1))

        return list(etkinlik_list), list(etkinlik_grup_list)

    def close_driver(self):
        self.driver.quit()


class BiletixEventDetails:
    def __init__(self):
        self.driver = self._setup_driver()


    @staticmethod
    def is_embedded_json_category(raw_cat: str) -> bool:
        """
        'category' alanının gömülü JSON içerip içermediğini saniyeler içinde
        belirler.  Boşlukları kırpar; '{' ile başlıyorsa JSON varsayar ve
        json.loads ile doğrulama yapar (hatalı JSON varsa False döner).
        """
        if not isinstance(raw_cat, str):
            return False

        candidate = raw_cat.lstrip()
        if not candidate.startswith("{"):
            return False

        try:
            json.loads(candidate)
            return True
        except json.JSONDecodeError:
            return False

    @staticmethod
    def connect_db():
        return pg8000.connect(
            database="Eventist",
            user="postgres",
            password="xekqiz-xegbyq-tawSu8",
            host="localhost",  # or your database host
            port=5432  # PostgreSQL default port
        )

    @staticmethod
    def upsert_event_with_history(event_data):
        connection = BiletixEventDetails.connect_db()
        cursor = connection.cursor()

        # Extract event details
        provider = event_data['provider']
        name = event_data['name']
        description = event_data['description']
        venue = event_data['venue']
        date = event_data['date']
        genre = event_data.get('genre', '')
        price_list = event_data.get('price_list', [])
        current_time = datetime.now()

        try:
            # Check if event already exists
            cursor.execute("""
                           SELECT id
                           FROM biletix_events
                           WHERE name = %s
                             AND venue = %s
                             AND date = %s
                           """, (name, venue, date))
            event_record = cursor.fetchone()

            if event_record:
                event_id = event_record[0]
                cursor.execute("""
                               UPDATE biletix_events
                               SET provider  = %s,
                                   description = %s,
                                   genre     = %s,
                                   last_seen = %s
                               WHERE id = %s
                               """, (provider, description, genre, current_time, event_id))
            else:
                cursor.execute("""
                               INSERT INTO biletix_events (provider, name, description, venue, date, genre, created_at,
                                                         last_seen)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                               """, (provider, name, description, venue, date, genre, current_time, current_time))
                event_id = cursor.fetchone()[0]

            # Fetch existing prices
            cursor.execute("""
                           SELECT category, price, sold_out, id
                           FROM biletix_prices
                           WHERE event_id = %s
                             AND is_active = TRUE
                           """, (event_id,))
            existing_prices = {row[0]: (row[1], row[2], row[3]) for row in cursor.fetchall()}

            # Track updated categories
            updated_categories = set()

            # Process new or updated prices
            for price in price_list:
                category = price['category']
                price_value = price['price']
                sold_out = price['sold_out']
                updated_categories.add(category)

                if category in existing_prices:
                    old_price, old_sold_out, price_id = existing_prices[category]
                    if (old_price, old_sold_out) != (price_value, sold_out):
                        # Save to history
                        cursor.execute("""
                                       INSERT INTO biletix_price_history (event_id, category, price, sold_out, change_date, change_type)
                                       VALUES (%s, %s, %s, %s, %s, %s)
                                       """, (event_id, category, old_price, old_sold_out, current_time, 'UPDATED'))

                        # Update current price
                        cursor.execute("""
                                       UPDATE biletix_prices
                                       SET price     = %s,
                                           sold_out  = %s,
                                           last_seen = %s
                                       WHERE id = %s
                                       """, (price_value, sold_out, current_time, price_id))
                else:
                    # New price category
                    cursor.execute("""
                                   INSERT INTO biletix_prices (event_id, category, price, sold_out, created_at, last_seen, is_active)
                                   VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                                   """, (event_id, category, price_value, sold_out, current_time, current_time))

                    # Log history for new price
                    cursor.execute("""
                                   INSERT INTO biletix_price_history (event_id, category, price, sold_out, change_date, change_type)
                                   VALUES (%s, %s, %s, %s, %s, %s)
                                   """, (event_id, category, price_value, sold_out, current_time, 'ADDED'))

            # Handle removed prices (not in the latest data)
            for category, (old_price, old_sold_out, price_id) in existing_prices.items():
                if category not in updated_categories:
                    # Mark price as inactive
                    cursor.execute("""
                                   UPDATE biletix_prices
                                   SET is_active = FALSE,
                                       last_seen = %s
                                   WHERE id = %s
                                   """, (current_time, price_id))

                    # Log to history
                    cursor.execute("""
                                   INSERT INTO biletix_price_history (event_id, category, price, sold_out, change_date, change_type)
                                   VALUES (%s, %s, %s, %s, %s, %s)
                                   """, (event_id, category, old_price, old_sold_out, current_time, 'REMOVED'))

            connection.commit()
            print(f"Event '{name}' processed with price history tracking.")

        except Exception as e:
            connection.rollback()
            print("Error:", e)
        finally:
            cursor.close()
            connection.close()

    def _setup_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        return webdriver.Chrome(service=Service(), options=options)

    def get_event_data_selenium(self, url):
        try:
            time.sleep(random.uniform(2, 5))  # Mimic user loading the page
            self.driver.get(url)
            self.driver.implicitly_wait(random.uniform(2, 5))
            html_content = self.driver.page_source
            return html_content
        except Exception as e:
            print(f"Error fetching data from {url}: {e}")
            return None

    def parse_performance_by_event_code_and_perf_code(self, url):
        #time.sleep(random.uniform(2, 4))  # Mimic user delay before fetching
        html_response = self.get_event_data_selenium(url)
        if not html_response:
            return None

        soup = BeautifulSoup(html_response, "html.parser")
        pre_tag = soup.find("pre")

        if not pre_tag:
            print("Error: JSON data not found!")
            return None

        try:
            json_data = json.loads(pre_tag.text)
            data = json_data.get("data", {})
            html_str = data.get("priceInfo", "")
            new_soup = BeautifulSoup(html_str, "html.parser")
            results = []

            last_text_div = ""

            # First: parse <div><span>...</span></div> structure
            for div in new_soup.find_all("div"):
                spans = div.find_all("span")

                if len(spans) < 2:
                    last_text_div = div.get_text(strip=True)
                    continue

                category_raw = spans[0].get_text(strip=True)
                price_text = spans[1].get_text(strip=True)

                match = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*TL", price_text)
                if match:
                    price = float(match.group(1).replace(",", "."))
                    sold_out = "tükendi" in price_text.lower()
                    category = category_raw if category_raw else last_text_div
                    results.append({
                        "category": category,
                        "price": price,
                        "sold_out": sold_out
                    })

            # Then: parse plain <br>-separated lines (fallback for no spans/divs)
            text_lines = soup.get_text(separator="\n").splitlines()
            for line in text_lines:
                line = line.strip()
                if not line:
                    continue

                match = re.match(r"(.+?)\s+([0-9]+(?:[.,][0-9]+)?)\s*TL", line)
                if match:
                    category = match.group(1).strip()
                    price = float(match.group(2).replace(",", "."))
                    sold_out = "tükendi" in line.lower()

                    # Avoid duplicates from <div> section
                    if not any(r["category"] == category and r["price"] == price for r in results):
                        results.append({
                            "category": category,
                            "price": price,
                            "sold_out": sold_out
                        })

            return results, data.get("active")

        except json.JSONDecodeError:
            print("Error: Invalid JSON format!")
            return None, None

    def parse_event_detail(self, url):
        #time.sleep(random.uniform(2, 4))
        html_response = self.get_event_data_selenium(url)
        if not html_response:
            return None

        soup = BeautifulSoup(html_response, "html.parser")
        pre_tag = soup.find("pre")

        if not pre_tag:
            print("Error: JSON data not found!")
            return None

        try:
            json_data = json.loads(pre_tag.text)
            data = json_data.get("data", {})
            return (
                data.get("eventDescription"),
                data.get("info"),
                data.get("eventCategory"),
                data.get("subCategory"),
                data.get("venueLatitude"),
                data.get("venueLongitude"),
            )
        except json.JSONDecodeError:
            print("Error: Invalid JSON format!")
            return None



    def parse_group_page_info(self, html_content):


        #time.sleep(random.uniform(3, 7))  # Human-like waiting before parsing
        soup = BeautifulSoup(html_content, "html.parser")
        pre_tag = soup.find("pre")

        if not pre_tag:
            print("Error: JSON data not found!")
            return

        try:
            json_data = json.loads(pre_tag.text)
        except json.JSONDecodeError:
            print("Error: Invalid JSON format!")
            return

        events = json_data.get("data", {}).get("events", [])
        if not events:
            print("Error: No event information found!")
            return

        for event in events:
            try:
                event_name = event["eventName"].strip()
                venue = event["venueName"]
                city = event["venueCity"]
                event_code = event["eventCode"]
                performance_code = event["performanceCode"]

                date = datetime.fromtimestamp(event["performanceDate"] / 1000).strftime('%Y-%m-%d %H:%M:%S')

                #event_url = f"https://www.biletix.com/performance/{event_code}/{performance_code}/TURKIYE/tr"
                event_detail_url = f"https://www.biletix.com/wbtxapi/api/v1/bxcached/event/getEventDetail/{event_code}/INTERNET/tr"
                perf_by_event_code_and_perf_code_url = f"https://www.biletix.com/wbtxapi/api/v1/bxcached/event/getPerformanceByEventCodeAndPerfCode/{event_code}/{performance_code}/INTERNET/tr"

                event_details = self.parse_event_detail(event_detail_url) or (None, None, None, None, None, None)
                price_info_and_active = self.parse_performance_by_event_code_and_perf_code(perf_by_event_code_and_perf_code_url) or (None, None)

                current_event = {
                    'provider': 'Biletix',
                    'name': event_name,
                    'description': event_details[0],
                    'venue': venue + " - " + city,
                    'date': date,
                    'genre': event_details[3],
                    'price_list': price_info_and_active[0],
                    #'event_url': event_url,
                    #'event_code': event_code,
                    #'performance_code': performance_code,
                    #'event_detail_url': event_detail_url,
                    #'perf_by_event_code_and_perf_code_url': perf_by_event_code_and_perf_code_url,

                    #'event_info': event_details[1],
                    #'event_category': event_details[2],
                    #'venue_latitude': event_details[4],
                    #'venue_longitude': event_details[5]
                }

                BiletixEventDetails.upsert_event_with_history(current_event)
                print(current_event)


            except KeyError as e:
                print(f"Error: Missing key in event data - {e}")



    def close(self):
        self.driver.quit()



def main():

    #url = "https://www.biletix.com/search/TURKIYE/tr?category_sb=MUSIC&date_sb=-1&city_sb=-1#!category_sb:MUSIC"
    url = "https://www.biletix.com/search/TURKIYE/tr?category_sb=MUSIC&date_sb=-1&city_sb=%C4%B0stanbul#!category_sb:MUSIC,city_sb:%C4%B0stanbul"
    info_loader = BiletixInfoLoader(url)
    info_loader.load_page()
    event_ids, group_ids = info_loader.extract_event_ids()
    info_loader.close_driver()

    event_detail_scraper = BiletixEventDetails()

    for group_id in group_ids:
        url = f"https://www.biletix.com/wbtxapi/api/v1/bxcached/event/getGroupPageInfo/{group_id}/INTERNET/tr"
        html_response = event_detail_scraper.get_event_data_selenium(url)
        event_detail_scraper.parse_group_page_info(html_response)

    event_detail_scraper.close()


if __name__ == "__main__":
    main()
