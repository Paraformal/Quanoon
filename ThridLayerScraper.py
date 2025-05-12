import os
import re
import json
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

BASE_DIR = "Final_Laws_Content"
OUTPUT_DIR = "Scraped_Laws"
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

def extract_text_with_tables(container):
    content = []
    for elem in container.children:
        if elem.name == 'table':
            table_data = []
            for row in elem.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                row_data = [cell.get_text(strip=True) for cell in cells]
                table_data.append(row_data)
            content.append({"type": "table", "data": table_data})
        elif elem.name in ['p', 'div', 'span', 'li', 'h2', 'h3', 'h4']:
            text = elem.get_text(separator="\n", strip=True)
            if text:
                content.append({"type": "text", "data": text})
        elif isinstance(elem, str):
            text = elem.strip()
            if text:
                content.append({"type": "text", "data": text})
    return content

def parse_law_page(url):
    url = normalize_url(url)
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            title_tag = soup.find("h2", id="litLaw")
            title = title_tag.get_text(strip=True) if title_tag else "No Title"

            metadata = {
                "type": soup.find("span", id="lblType").text.strip() if soup.find("span", id="lblType") else None,
                "number": soup.find("span", id="lblNumber").text.strip().replace("رقم ", "") if soup.find("span", id="lblNumber") else None,
                "date": soup.find("span", id="lblDate").text.strip().replace("تاريخ :", "") if soup.find("span", id="lblDate") else None,
                "oj_number": soup.find("span", id="divOJNumber").text.strip().replace("عدد الجريدة الرسمية: ", "") if soup.find("span", id="divOJNumber") else None,
                "oj_publish_date": soup.find("span", id="divOJPublishDate").text.strip().replace("تاريخ النشر: ", "") if soup.find("span", id="divOJPublishDate") else None,
                "oj_page": soup.find("span", id="divOJPage").text.strip().replace("الصفحة: ", "") if soup.find("span", id="divOJPage") else None
            }

            notes_holder = soup.find("div", id="NotesHolders")
            preamble = notes_holder.get_text("\n", strip=True) if notes_holder else ""

            articles_div = soup.find("div", id="divTreeDetails")
            articles = []
            if articles_div:
                article_headers = articles_div.find_all("h2")
                article_contents = articles_div.find_all("div", class_="text-1")
                for h2, content_div in zip(article_headers, article_contents):
                    if "المادة" in h2.text:
                        number = h2.text.split("المادة")[-1].strip()
                        content = extract_text_with_tables(content_div)
                        articles.append({"number": number, "content": content})

            signature = soup.find("div", id="signatureLaw")
            signature_text = signature.get_text("\n", strip=True) if signature else ""

            return {
                "url": url,
                "title": title,
                "metadata": metadata,
                "preamble": preamble,
                "articles": articles,
                "signature": signature_text
            }
        except Exception as e:
            last_exc = e
            time.sleep(2 ** attempt)
    raise last_exc

def process_law_url(url, index):
    try:
        law_data = parse_law_page(url)
        law_number = law_data['metadata'].get('number') or f"{index+1:05d}"
        safe_filename = sanitize_filename(law_number)
        return index, safe_filename, law_data, None
    except Exception as e:
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
