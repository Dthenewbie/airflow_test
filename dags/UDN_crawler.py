from airflow.decorators import dag, task
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
import time
from tasks.insert_db import save_to_caseprocessing
import uuid
from utils.request_check import request_with_retry
from utils.selenium_setting import setup_driver


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="UDN_news_crawler",
    default_args=default_args,
    description="Scrapes news content and stores in MySQL",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["web crawler", "UDN", "case processing"]
)
def UDN_news_scraper_pipeline():
    def scrape_news_details(detail_url, retries=3) -> str:
        """進入新聞詳細頁，帶重試機制爬取內文內容。"""
        for attempt in range(retries):
            try:
                response = request_with_retry(detail_url)
                soup = BeautifulSoup(response.text, "html.parser")
                paragraphs = soup.select("div.article-content__paragraph p")
                if not paragraphs:
                    raise ValueError("No content found on the page.")

                content = "\n".join([p.text for p in paragraphs[:-2]])
                break
            except Exception as e:
                print(f"Error while scraping {detail_url}: {e}")
                print(f"Retrying {detail_url} (Attempt {attempt + 1}/{retries})")
                content = f"Error: {str(e)}"
        return content

    @task()
    def scrape_main_page():
        """爬取主頁內容，並進一步進入每個新聞的詳細頁。"""
        url = "https://udn.com/search/tagging/2/%E8%A9%90%E9%A8%99%E9%9B%86%E5%9C%98"
        driver = setup_driver()
        driver.get(url)

        processed_urls = set()
        all_results = []
        error_log = []

        try:
            # while True:
            for _ in range(2):
                news_blocks = driver.find_elements(By.CSS_SELECTOR, "div.story-list__news")
                if not news_blocks:
                    print("No news blocks found.")
                    break

                for block in news_blocks:
                    try:
                        title_element = block.find_element(By.CSS_SELECTOR, "div.story-list__text h2 a")
                        date_element = block.find_element(By.CSS_SELECTOR, "div.story-list__info time.story-list__time")
                        link_element = block.find_element(By.CSS_SELECTOR, "div.story-list__text h2 a[href]")

                        title = title_element.text
                        date = date_element.text
                        detail_url = link_element.get_attribute("href")

                        if detail_url in processed_urls:
                            continue

                        processed_urls.add(detail_url)

                        # 進入新聞詳細頁，爬取內文
                        content = scrape_news_details(detail_url)
                        news_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, content))

                        if "Failed" in content or "Error" in content:
                            error_log.append({"URL": detail_url, "Error": content})
                        else:
                            all_results.append({
                                "ID": news_id,
                                "Title": title,
                                "Reported_Date": date.split(" ")[0],
                                "Content": content,
                                "Url": detail_url,
                                "Area": None,
                                "Status": 0
                            })
                            print(f"Scraped: {title} ({date})")
                    except NoSuchElementException:
                        continue
                    except Exception as e: 
                        print(f"Error while processing news block: {e}")

                # 模擬滾動加載新內容
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                print("Scrolling to load more content...")

                # 檢查是否有新內容
                new_news_blocks = driver.find_elements(By.CSS_SELECTOR, "div.story-list__news")
                if len(new_news_blocks) <= len(news_blocks):
                    print("No more new content. Stopping the scrape.")
                    break

        finally:
            driver.quit()

        return all_results

    # Define task dependencies
    scraped_data = scrape_main_page()
    save_to_caseprocessing(scraped_data)

UDN_news_scraper_pipeline()
