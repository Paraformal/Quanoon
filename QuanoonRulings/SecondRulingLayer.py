import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import logging

logging.basicConfig(filename="scraping_rulings_content.log", level=logging.INFO,
                    format='%(asctime)s - %(message)s')
logger = logging.getLogger()

input_dir = "First_Rulings_Layer"
output_dir = "Second_Rulings_Layer"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

def get_view_link(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            a_tag = soup.find('a', id='MainContent_viewLink', href=True)
            if a_tag:
                href = a_tag['href']
                full_link = f"http://77.42.251.205/{href.lstrip('/')}"
                return full_link
        return None
    except Exception as e:
        logger.error(f"Error scraping {url}: {str(e)}")
        return None

def process_batch(urls, max_workers=5):
    view_links = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(get_view_link, url) for url in urls]
        for future in as_completed(futures):
            result = future.result()
            if result:
                view_links.append(result)
    return view_links

def process_year(year, urls, batch_size=10, max_workers=5):
    logger.info(f"STARTING year {year} with {len(urls)} URLs")
    all_view_links = []
    for start in range(0, len(urls), batch_size):
        batch = urls[start:start+batch_size]
        logger.info(f"Processing batch {start // batch_size + 1} for year {year} ({len(batch)} URLs)")
        batch_links = process_batch(batch, max_workers=max_workers)
        all_view_links.extend(batch_links)
    output_file = os.path.join(output_dir, f"{year}.txt")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(all_view_links))
    logger.info(f"FINISHED year {year} with {len(all_view_links)} links")
    return year, len(all_view_links)

def process_year_wrapper(year, batch_size=10, max_workers=5):
    input_file = os.path.join(input_dir, f"{year}.txt")
    if not os.path.exists(input_file):
        logger.info(f"No input file for year {year}, skipping.")
        return year, 0
    with open(input_file, 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip()]
    if not urls:
        logger.info(f"No URLs found in input file for year {year}, skipping.")
        return year, 0
    return process_year(year, urls, batch_size=batch_size, max_workers=max_workers)

def process_all_years_parallel(year_workers=3, batch_size=10, link_workers=5):
    years = sorted(
        [int(fname.split('.')[0]) for fname in os.listdir(input_dir) if fname.endswith('.txt')],
        key=lambda x: x
    )
    with ThreadPoolExecutor(max_workers=year_workers) as executor:
        futures = {executor.submit(process_year_wrapper, year, batch_size, link_workers): year for year in years}
        for future in as_completed(futures):
            year = futures[future]
            try:
                year_processed, count = future.result()
                logger.info(f"Year {year_processed} completed with {count} links saved.")
            except Exception as e:
                logger.error(f"Error processing year {year}: {str(e)}")

if __name__ == "__main__":
    process_all_years_parallel(year_workers=3, batch_size=10, link_workers=5)
