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
    dag_id="New_Taipei_Police_Department_crawler",
    default_args=default_args,
    description="A web scraping and data pipeline DAG for Case_processing table",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=["web crawler", "New_Taipei_Police_Department", "case processing"]
)
def New_Taipei_Police_Department_scraper_pipeline():
    def getPageContent(soup) -> dict:
        try:
            Title = soup.select_one("div.pageHeader h2").text.strip()
            Date = soup.select_one("time").text.strip()
            Content = soup.select("article.cpArticle p")
            content_text = "".join([ele.text for ele in Content])
            uuid_str = str(uuid.uuid3(uuid.NAMESPACE_DNS, content_text))
            # Created_time = str(datetime.now())
            data = {
                "ID": uuid_str, 
                "Title": Title, 
                "Reported_Date": Date,
                "Content": content_text, 
                "Area": None,
                "Status": 0}
            print(data)
        except:
            print('fail to scraped')
        return data
    @task
    def Scrape_page():
        result = []
        base_url = "https://www.police.ntpc.gov.tw/"
        url = "https://www.police.ntpc.gov.tw/lp-3431-1-xCat-01-1-60.html"
        response = request_with_retry(url)
        soup = BeautifulSoup(response.text, "html.parser")
        try:
            current_page = url
            # while True:
            for _ in range(2): 
                Url_list = soup.select("body > main > section.list > ul > li > a")
                for url_ele in Url_list:
                    try:
                        url_tail = url_ele["href"]
                        url = base_url + url_tail
                        response_block = request_with_retry(url)
                        soup_block = BeautifulSoup(response_block.text, "html.parser")
                        data = getPageContent(soup_block)
                        data["Url"] = url
                        result.append(data)
                        print(f"scraped: {url}")
                    except:
                        print("url is invailid")
                next_page_tail = soup.select_one("li.next a")["href"]
                next_page = base_url + next_page_tail
                # # 如果下一頁等於當前頁面，則停止
                # if current_page == next_page:
                #     print("no more page")
                #     break
                # current_page = next_page
                # 爬取下一頁
                response = request_with_retry(next_page)
                soup = BeautifulSoup(response.text, "html.parser")
                print(f"entering next page: {next_page}")
        except Exception as e:
            print(e)
        
        return result
    
    @task
    def data_transformation(result) -> dict:
        df = pd.DataFrame(result)
        df["Content"] = df["Content"].apply(clean_content)
        df['Reported_Date'] = df['Reported_Date'].apply(lambda x: str(int(x.split("-")[0])+1911) + "-" + x.split("-")[1] + "-" + x.split("-")[2])
        result_formated = df.to_dict(orient="records")
        return result_formated

    # Task dependencies
    result = Scrape_page()
    result_formated = data_transformation(result)
    save_to_caseprocessing(result_formated)

# Instantiate the DAG
New_Taipei_Police_Department_scraper_pipeline()