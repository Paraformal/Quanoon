import os
import re
import json
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

BASE_DIR = "Second_Rulings_Layer"
OUTPUT_DIR = "Scraped_Rulings"
FAILED_LOG = "logs/failed_laws.txt"
MAX_WORKERS = 20
MAX_RETRIES = 3
REQUEST_DELAY = 0.1

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(FAILED_LOG), exist_ok=True)


def sanitize_filename(name, max_length=50):
    name = re.sub(r'[\\/*?:"<>|]', "_", name).strip().rstrip('.')
    return name[:max_length].strip()


def normalize_url(url):
    if url.startswith("http://77.42.251.205") and not url.startswith("http://77.42.251.205/"):
        url = url.replace("http://77.42.251.205", "http://77.42.251.205/", 1)
    return url


def parse_law_page(url):
    url = normalize_url(url)
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract title from <title> tag
            title = soup.title.string.strip() if soup.title else "No Title"

            # Extract card info fields by their IDs
            card_info = {
                "court": soup.find(id='lblcourtName').text.strip() if soup.find(id='lblcourtName') else None,
                "number": soup.find(id='lblNumber').text.strip() if soup.find(id='lblNumber') else None,
                "year": soup.find(id='lblyear').text.strip() if soup.find(id='lblyear') else None,
                "session_date": soup.find(id='lblDate').text.strip() if soup.find(id='lblDate') else None,
                "chief": soup.find(id='lblJudge').text.strip() if soup.find(id='lblJudge') else None,
                "members": soup.find(id='lblMembers').text.strip() if soup.find(id='lblMembers') else None
            }

            # Extract ruling text preserving line breaks
            ruling_text_tag = soup.find(id='rulingtext')
            ruling_text = ruling_text_tag.get_text(separator='\n', strip=True) if ruling_text_tag else None

            return {
                "url": url,
                "title": title,
                "card_info": card_info,
                "ruling_text": ruling_text
            }
        except Exception as e:
            last_exc = e
            time.sleep(2 ** attempt)
    raise last_exc


def process_law_url(url, index):
    try:
        law_data = parse_law_page(url)
        law_number = law_data.get('card_info', {}).get('number') or f"{index+1:05d}"
        safe_filename = sanitize_filename(law_number)
        print(f"[INFO] Processed URL {url} with number {law_number}")
        return index, safe_filename, law_data, None
    except Exception as e:
        print(f"[ERROR] Failed on {url}: {e}")
        return index, None, None, f"Failed on {url}: {e}"


def process_year_file(year_filename):
    year = os.path.splitext(year_filename)[0]
    year_folder = os.path.join(OUTPUT_DIR, year)
    os.makedirs(year_folder, exist_ok=True)

    year_path = os.path.join(BASE_DIR, year_filename)
    with open(year_path, "r", encoding="utf-8") as f:
        all_urls = [normalize_url(line.strip()) for line in f if line.strip()]

    failed = []
    results = [None] * len(all_urls)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_law_url, url, idx): (idx, url) for idx, url in enumerate(all_urls)}
        with tqdm(total=len(futures), desc=f"Year {year}") as pbar:
            for future in as_completed(futures):
                idx, url = futures[future]
                try:
                    _, safe_filename, law_data, error = future.result()
                    if error:
                        failed.append(error)
                    else:
                        results[idx] = (safe_filename, law_data)
                except Exception as e:
                    failed.append(f"Unexpected error on {url}: {e}")
                pbar.update(1)
                time.sleep(REQUEST_DELAY)

    # Track filenames to avoid duplicates
    filename_counts = {}

    for idx, item in enumerate(results):
        if item is None:
            continue
        safe_filename, law_data = item
        if not law_data:
            print(f"[WARNING] Empty data for index {idx}, skipping save.")
            continue

        # Ensure unique filename
        count = filename_counts.get(safe_filename, 0)
        if count > 0:
            unique_filename = f"{safe_filename}_{count}"
        else:
            unique_filename = safe_filename
        filename_counts[safe_filename] = count + 1

        json_path = os.path.join(year_folder, f"{unique_filename}.json")
        with open(json_path, "w", encoding="utf-8") as jf:
            json.dump(law_data, jf, ensure_ascii=False, indent=2)

    if failed:
        with open(FAILED_LOG, "a", encoding="utf-8") as flog:
            flog.write(f"\n--- {year} ---\n")
            for err in failed:
                flog.write(err + "\n")


def main():
    for file in sorted(os.listdir(BASE_DIR)):
        if file.endswith(".txt"):
            process_year_file(file)


if __name__ == "__main__":
    main()
