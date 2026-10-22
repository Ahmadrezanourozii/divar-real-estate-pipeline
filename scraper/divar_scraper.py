import time
import pandas as pd
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


class DivarScraper:
    def __init__(self, url, export_file_name, city: str, total_ads_needed=1000, checkpoint_interval=50):
        self.url = url
        self.total_ads_needed = total_ads_needed
        self.checkpoint_interval = checkpoint_interval
        self.data_list = []
        self.seen_ads = set()
        self.driver = self._initialize_driver()
        self.export_file_name = export_file_name
        self.city = city

    def _initialize_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    def extract_details(self):
        details = {"metrage": "", "build_year": "", "rooms": "", "total_price": "", "price_per_meter": "", "floor": "",
                   "location": "", "city": self.city}

        try:
            location_element = self.driver.find_element(By.CLASS_NAME, "kt-page-title__subtitle")
            location_parts = location_element.text.split("،")
            details["location"] = location_parts[-1].strip() if len(location_parts) > 1 else ""
        except:
            pass

        try:
            details_table = self.driver.find_element(By.CLASS_NAME, "kt-group-row")
            details_elements = details_table.find_elements(By.CLASS_NAME, "kt-group-row-item__value")

            if len(details_elements) > 0 and "متراژ" in details_table.text:
                details["metrage"] = details_elements[0].text
            if len(details_elements) > 1 and "ساخت" in details_table.text:
                details["build_year"] = details_elements[1].text
            if len(details_elements) > 2 and "اتاق" in details_table.text:
                details["rooms"] = details_elements[2].text
        except:
            pass

        try:
            price_rows = self.driver.find_elements(By.CLASS_NAME, "kt-base-row")
            metadata = {}
            for row in price_rows:
                try:
                    label_element = row.find_element(By.CLASS_NAME, "kt-unexpandable-row__title")
                    value_element = row.find_element(By.CLASS_NAME, "kt-unexpandable-row__value")
                    label = label_element.text.strip()
                    value = value_element.text.strip()

                    if "قیمت کل" in label:
                        details["total_price"] = value
                    elif "قیمت هر متر" in label:
                        details["price_per_meter"] = value
                    elif "طبقه" in label:
                        details["floor"] = value
                except:
                    pass
        except:
            pass

        return details

    def scrape(self):
        self.driver.get(self.url)
        time.sleep(3)

        while len(self.data_list) < self.total_ads_needed:
            ads = self.driver.find_elements(By.CLASS_NAME, "kt-post-card__title")

            for ad in ads:
                try:
                    title = ad.text
                    link = ad.find_element(By.XPATH, "./ancestor::a").get_attribute("href")
                    price = ad.find_element(By.XPATH, "./following-sibling::div").text

                    if link not in self.seen_ads:
                        self.seen_ads.add(link)
                        self.driver.get(link)
                        time.sleep(1.5)

                        details = self.extract_details()
                        details.update({"title": title, "price": price, "url": link})
                        self.data_list.append(details)
                        print("Extracted:", details)

                        if len(self.data_list) % self.checkpoint_interval == 0:
                            pd.DataFrame(self.data_list).to_excel(self.export_file_name, index=False)
                            print(f"Checkpoint saved: {len(self.data_list)} records written to Excel")

                        self.driver.back()
                        time.sleep(1)

                    if len(self.data_list) >= self.total_ads_needed:
                        break

                except Exception as e:
                    print("Error extracting data:", e)

            self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
            time.sleep(3)

            try:
                more_button = self.driver.find_element(By.XPATH, "//*[@id='post-list-container-id']/div[2]/div/button")
                if more_button.is_displayed():
                    more_button.click()
                    time.sleep(1.5)
            except:
                pass

        self.driver.quit()


if __name__ == "__main__":
    CITIES = ['tehran' ,'mashhad' ,'isfahan']
    TOTAL_ADS = 10
    CHECKPOINT_INTERVAL = 5
    for city in CITIES:
        URL = f"https://divar.ir/s/{city}/buy-apartment"
        export_file_name = f"/home/airflow/data/real_estate_{city}.xlsx"
        scraper = DivarScraper(URL, export_file_name, city, TOTAL_ADS, CHECKPOINT_INTERVAL)
        scraper.scrape()
        pd.DataFrame(scraper.data_list).to_excel(export_file_name, index=False)