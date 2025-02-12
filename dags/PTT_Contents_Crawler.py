from airflow.decorators import dag, task
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import uuid
from utils.selenium_setting import setup_driver
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
    dag_id="PTT_crawler",
    default_args=default_args,
    description="A web scraping and data pipeline DAG for Case_processing table",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=["web crawler", "PTT", "case processing"]
)
def PTT_scraper_pipeline():
    def get_soup(driver, url):
        """
        取得網頁內容並於遇到成年問題時點擊確認
        """
        driver.get(url)
        if "over18" in driver.current_url:
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.NAME, "yes"))).click()
        time.sleep(1)  # 等待頁面加載
        page_source = driver.page_source
        return BeautifulSoup(page_source, "html.parser")

    def get_article_links(soup) -> list:
        """
        獲取頁面各篇文章連結
        """
        articles = soup.select("div.r-ent div.title a")
        return [a["href"] for a in articles]
    
    def get_article_content(base_url, article_url) -> dict:
        """
        提取文章所需欄位
        """
        driver_content = setup_driver()
        try:
            soup = get_soup(driver_content, f"{base_url}{article_url}")
            titleTag = soup.select_one("meta[property='og:title']")
            title = titleTag["content"] if titleTag else "No Title"
            authorTag = soup.select_one("span.article-meta-value")
            author = authorTag.text if authorTag else "No Author"
            dateTag = soup.select("span.article-meta-value")
            date = dateTag[-1].text if dateTag else "No Date"
            content = soup.select_one(
                "#main-content").text.split("※ 發信站: 批踢踢實業坊(ptt.cc)")[0].strip()
            uuid_str = str(uuid.uuid3(uuid.NAMESPACE_DNS, content))
        except Exception as e:
            print(f"get_article_content:{e}")
        finally:
            driver_content.quit()
        return {
            "ID": uuid_str,
            "Title": title,
            "author": author,
            "Reported_Date": date,
            "Content": content, 
            "Url": f"{base_url}{article_url}",
            "Area": None}
    @task
    def get_data_list():
        """
        爬取所有頁面的文章內容
        """
        base_url = "https://www.ptt.cc"
        start_url = f"{base_url}/bbs/Bunco/index.html"
        all_articles = []
        current_url = start_url
        driver = setup_driver()
        try:
            # while True:
            for _ in range(1):
                soup = get_soup(driver, current_url)
                article_links = get_article_links(soup)
                for link in article_links:
                    try:
                        article_content = get_article_content(base_url, link)
                        if article_content['Title']!="No Title":
                            all_articles.append(article_content)
                            print(f"已爬取: {article_content['Title']}")
                    except Exception as e:
                        print(f"Error fetching article {link}: {e}")
                
                # 找到上一頁的連結
                paging = soup.select("div.btn-group.btn-group-paging a")
                prev_page_link = paging[1]["href"] if len(paging) > 1 else None
                
                if prev_page_link and "index" in prev_page_link:
                    current_url = f"{base_url}{prev_page_link}"
                else:
                    break
        except Exception as e:
            print(e)

        finally:
            driver.quit()
        return all_articles
    def clean_content(text):
        # 去除特殊符號與非中文字元
        cleaned_text = re.sub(r'[^一-龥A-Za-z0-9，。、！？；：「」（）\s]', '', text)
        # 去除多餘空白行
        cleaned_text = re.sub(r'\n+', '\n', cleaned_text).strip()
        return cleaned_text
    @task
    def data_transformation(result):
        df = pd.DataFrame(result)
        # 將字串轉換為日期格式，再格式化成目標格式
        df['Reported_Date'] = pd.to_datetime(df['Reported_Date']).dt.strftime('%Y-%m-%d')
        df['Content'] = df['Content'].apply(clean_content)
        result_formated = df.to_dict(orient="records")
        return result_formated
        
    # Task dependencies
    result = get_data_list()
    result_formated = data_transformation(result)
    save_to_caseprocessing(result_formated)

# Instantiate the DAG
PTT_scraper_pipeline()