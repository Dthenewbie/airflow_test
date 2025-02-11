from datetime import datetime, timedelta
from airflow.decorators import dag, task
from bs4 import BeautifulSoup
import requests
import time
import random
import re
import uuid
from tasks.insert_db import save_to_caseprocessing

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="ETtoday_crawler",
    default_args=default_args,
    description="A web scraping and data pipeline DAG for Case_processing table",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=["web crawler", "ETtoday", "case processing"]
)
def ETtoday_news_scraper_pipeline():
    @task
    def scrape_website() -> list:
            url_base = "https://www.ettoday.net/news_search/doSearch.php?keywords=%E8%A9%90%E9%A8%99&idx=1&page="
            all_data = []
            processed_data = set()
            # pages = 100
            pages = 2
            for page in range(1, pages+1):  # 測試限制 2 頁
                url = f"{url_base}{page}"
                print(f"Scraping page {page}: {url}")
                try:
                    response = requests.get(url)
                    time.sleep(random.uniform(1, 2))
                    soup = BeautifulSoup(response.text, "html.parser")
                    page_data = scrape_page(soup)
                    for data in page_data:
                        if data["title"] not in processed_data:
                            processed_data.add(data["title"])
                            all_data.append(data)
                    print(f"Scraped page {page} successfully.")
                except Exception as e:
                    print(f"Error scraping page {page}: {e}")
            return all_data

    def scrape_page(soup):
        data = []
        titles = soup.select('div.box_2 h2 a')
        dates = soup.select('span.date')

        for title, date in zip(titles, dates):
            create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            pattern = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}')
            date_text = re.search(pattern, date.text).group()
            retries = 3
            for round in range(retries):
                try:
                    time.sleep(1)
                    response = requests.get(title["href"])
                    soup = BeautifulSoup(response.text, "html.parser")
                    content = soup.select("div.story p")
                    content_text = "".join([ele.text for ele in content[3:]])
                    uuid_str = str(uuid.uuid3(uuid.NAMESPACE_DNS, content_text))
                # 處理進入各篇新聞一覽頁面連結時，爬取內容出現錯誤，重新嘗試
                except Exception as e:
                    if round == retries + 1:    
                        print(f"Error fetching content: {e}")
                        content_text = ""
                        uuid_str = ""
                    else:
                        print(f"Retrying {title['href']} (Attempt {round + 1}/{retries})")
                        time.sleep(random.randint(3))

            item = {
                "ID": uuid_str,
                "Title": title.text,
                "Reported_Date": date_text.split(" ")[0],  # 提取日期部分
                "Content": content_text,
                "Url": title["href"],
                "Area": "",
                "Status": 0  # 初始狀態為 0
            }
            data.append(item)
        return data

    # Task dependencies
    scraped_data = scrape_website()
    save_to_caseprocessing(scraped_data)

# Instantiate the DAG
ETtoday_news_scraper_pipeline()
