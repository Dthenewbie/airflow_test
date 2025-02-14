from datetime import datetime, timedelta
from airflow.decorators import dag, task
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import uuid
from tasks.insert_db import save_to_caseprocessing
from utils.text_handler import clean_content
from utils.request_check import request_with_retry


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
    dag_id="pts_news_crawler",
    default_args=default_args,
    description="PTS News Web Scraper with MySQL Integration",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=["web crawler", "PTS", "case processing"]
)
def PTS_news_scraper_pipeline():
    @task
    def scrape_website() -> list:
        url_start = "https://news.pts.org.tw/tag/128?page="
        url_tail = "&type=new"
        seen_ID = set()
        all_data = []
        pagenum = 2  # 測試限制爬取 2 頁

        for page in range(1, pagenum + 1):
            url = url_start + str(page) + url_tail
            print(f"Scraping page {page}: {url}")
            response = request_with_retry(url)
            soup = BeautifulSoup(response.text, "html.parser")
            # 爬取當前頁面資料
            page_data = scrape_page(soup)
            for data in page_data:
                if data["ID"] not in seen_ID:
                    seen_ID.add(data["ID"])
                    all_data.append(data)
            print(f"{len(all_data)} data scraped.")
        return all_data

    def scrape_page(soup):
        data = []
        # 爬取所有目標資料
        titles = soup.select('div.pt-2.pt-md-0 h2 a')
        dates = soup.select('div.news-info time')
        for title, date in zip(titles, dates):
            create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 進入文章內容
            response = request_with_retry(title["href"])
            soup = BeautifulSoup(response.text, "html.parser")
            try:
                content = soup.select_one('div.post-article.text-align-left').text.replace("\n", "")
                uuid_str = str(uuid.uuid3(uuid.NAMESPACE_DNS, content))
            except Exception as e:
                print(f"Error fetching content: {e}")
                content = None
                uuid_str = None
            item = {
                "ID": uuid_str,
                "Title": title.text.replace("\n", ""),
                "Reported_Date": date.text.split(" ")[0],
                "Content": content,
                "Url": title["href"],
                "Area": None,
                "Status": 0
            }
            data.append(item)
            time.sleep(random.uniform(1, 2))
        return data
    
    @task
    def data_transformation(result):
        df = pd.DataFrame(result)
        # 將字串轉換為日期格式，再格式化成目標格式
        df['Content'] = df['Content'].apply(clean_content)
        result_formated = df.to_dict(orient="records")
        return result_formated


    # Task dependencies
    scraped_data = scrape_website()
    result_formated = data_transformation(scraped_data)
    save_to_caseprocessing(result_formated)

# Instantiate the DAG
PTS_news_scraper_pipeline()
