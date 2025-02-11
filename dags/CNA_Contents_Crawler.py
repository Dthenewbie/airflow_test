from datetime import datetime, timedelta
from airflow.decorators import dag, task
from bs4 import BeautifulSoup
import time
import random
import uuid
from tasks.insert_db import save_to_caseprocessing
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
    dag_id="CNA_Contents_Crawler",
    default_args=default_args,
    description="CNA News Web Scraper with MySQL Integration",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=["web crawler", "PTS", "case processing"]
)
def CNA_news_scraper_pipeline():
    def parse_article_links(html) -> list:
        soup = BeautifulSoup(html.text, 'html.parser')
        articles = soup.select("#jsMainList a")
        return ["https://www.cna.com.tw" + a['href'] for a in articles]
    
    def fetch_article_content(url) -> dict:
        response = request_with_retry(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            title = soup.title.text.strip()
            date_tag = soup.select_one(".updatetime")
            date = date_tag.text.strip() if date_tag else "無時間"
            inner_text = soup.find("div", {"class": "paragraph"})
            if not inner_text:
                content_text = None
            paragraphs = inner_text.find_all('p')
            content_text = " ".join(p.text for p in paragraphs)
            uuid_str = str(uuid.uuid3(uuid.NAMESPACE_DNS, content_text))
            article = {"ID":uuid_str, 
                    "Title": title, 
                    "Reported_Date": date.split(" ")[0], 
                    "Content": content_text, 
                    "Url": url,
                    "Area":None}
        except Exception as e:
            article = None
            print(e)

        return article
    @task
    def scrape_page():
        search_url = "https://www.cna.com.tw/search/hysearchws.aspx?q=%E8%A9%90%E9%A8%99"
        html = request_with_retry(search_url)
        if not html:
            return

        article_links = parse_article_links(html)
        news = []

        for link in article_links:
            article = fetch_article_content(link)
            if article:
                print(f"scraped: {article['Title']}")
                news.append(article)

        return news
    # Task dependencies
    scraped_data = scrape_page()
    save_to_caseprocessing(scraped_data)

# Instantiate the DAG
CNA_news_scraper_pipeline()