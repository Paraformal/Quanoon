import requests
from bs4 import BeautifulSoup
import time
import os

output_dir = os.path.join(os.getcwd(), "First_Rulings_Layer")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
print(f"Output directory: {output_dir}")

base_url = (
    "http://77.42.251.205/AdvancedRulingSearch.aspx?searchText=&AndOr=AND&typeid=0"
    "&courtID=0&depid=0&rulNumber=0&rulYear={year}&judjes=&desicionmonth=0"
    "&DesicionDay=0&DesicionYear=0&&&pageNumber={page_number}&language=ar"
)

def scrape_urls_for_year(year):
    page_number = 1
    all_urls = []
    while True:
        url = base_url.format(year=year, page_number=page_number)
        print(f"[{year}] Fetching page {page_number}...")
        try:
            response = requests.get(url, timeout=15)
        except Exception as e:
            print(f"[{year}] ERROR: {e}")
            break
        if response.status_code != 200:
            print(f"[{year}] HTTP {response.status_code} - skipping year.")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        main_div = soup.find('div', {'id': 'MainContent_mainLegTr'})
        if not main_div:
            print(f"[{year}] No results div found on page {page_number}.")
            break

        found = False
        for wrap in main_div.find_all('div', class_='extra-wrap'):
            a_tag = wrap.find('a', href=True)
            if a_tag:
                full_link = f"http://77.42.251.205/{a_tag['href'].lstrip('/')}"
                all_urls.append(full_link)
                print(f"[{year}] Found URL: {full_link}")
                found = True

        if not found:
            print(f"[{year}] No URLs found on page {page_number}.")
            break

        # Pagination: check if there's a next page
        pager = soup.find('ul', {'id': 'MainContent_Pager2_ulPager'})
        next_page = False
        if pager:
            active_li = pager.find('li', class_='active')
            if active_li:
                next_li = active_li.find_next_sibling('li')
                if next_li and 'disabled' not in next_li.get('class', []):
                    page_number += 1
                    next_page = True
        if not next_page:
            break

        time.sleep(1.5)  # Be polite

    return all_urls

def save_urls(year, urls):
    if urls:
        file_path = os.path.join(output_dir, f"{year}.txt")
        with open(file_path, "w", encoding="utf-8") as f:
            for url in urls:
                f.write(url + "\n")
        print(f"[{year}] Saved {len(urls)} URLs to {file_path}")
    else:
        print(f"[{year}] No URLs found, no file saved.")

if __name__ == "__main__":
    for year in range(1900, 2025):
        print(f"Scraping year {year}...")
        urls = scrape_urls_for_year(year)
        save_urls(year, urls)
