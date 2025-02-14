from airflow.decorators import dag, task
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import json
import time
import uuid
import re
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
    dag_id="Taoyuan_Police_Department_Crawler_crawler",
    default_args=default_args,
    description="A web scraping and data pipeline DAG for Case_processing table",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=["web crawler", "Taoyuan_Police_Department_Crawler", "case processing"]
)
def Taoyuan_Police_Department_scraper_pipeline():
    def scam_info(url) -> list:
        """抓取單一頁面的所有標題、時間與文章連結"""
        base_url = "https://www.typd.gov.tw/"
        response = request_with_retry(url)
        soup = BeautifulSoup(response.text, "html.parser")

        titles = soup.select("div h2")
        dates = soup.select("div span")
        links = soup.select("ul.ul_newslist022 a")

        page_data = []  # 存放當前頁面的所有數據

        for title, date, link in zip(titles, dates, links):
            # 取得文章連結
            full_link = base_url + link["href"]

            # 進入文章連結取得內文
            response = request_with_retry(full_link)
            soup = BeautifulSoup(response.text, "html.parser")
            p_tags = soup.select("h3 p")

            # 將段落文字合併為一個字串
            p_list = [p.text.strip() for p in p_tags if p.text.strip()]
            content = " ".join(p_list)

            # 生成 UUID
            unique_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, content))

            #生成目前日期時間
            now = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

            # 將數據儲存為字典並保證順序
            item = {
                "ID": unique_id,
                "Title": title.text.strip(),
                "Reported_Date": date.text.strip()[5:],
                "Content": content,
                "Url": full_link,
                "Area": None
            }

            page_data.append(item)

        return page_data
    
    @task
    def page_iter():
        all_data = []  # 用來存放所有頁面的數據
        page = 5
        for i in range(1, page+1):  # 抓取第 1 到第 5 頁
            url = f"https://www.typd.gov.tw/index.php?catid=551&cid=25&action=index&pg={i}#gsc.tab=0"
            print(f"正在抓取第 {i} 頁的數據...")
            try:
                page_data = scam_info(url)
                all_data.extend(page_data)  # 將每一頁的數據加到總數據中
            except Exception as e:
                print(f"抓取第 {i} 頁時發生錯誤: {e}")
                break

        return all_data
    @task
    def data_transformation(result):
        df = pd.DataFrame(result)
        # 將字串轉換為日期格式，再格式化成目標格式
        df['Content'] = df['Content'].apply(clean_content)
        result_formated = df.to_dict(orient="records")
        return result_formated
    
    # Task dependencies
    result = page_iter()
    result_formated = data_transformation(result)
    save_to_caseprocessing(result_formated)

# Instantiate the DAG
Taoyuan_Police_Department_scraper_pipeline()


