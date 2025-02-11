import requests
import time

def request_with_retry(url, retries=3) -> requests.Response:
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36"}
    for attempt in range(retries):
        response = requests.get(url, headers=headers)
        time.sleep(1)
        if response.status_code != 200:
            print(f"請求失敗，status code: {response.status_code}")
            print(f"Retrying {url} (Attempt {attempt + 1}/{retries})")
            time.sleep(1)
            response = None
        else:
            break
    return response