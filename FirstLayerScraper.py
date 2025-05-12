# import requests
# from bs4 import BeautifulSoup
# import time
# import os
# import logging
# from datetime import datetime

# # Setup logging
# logging.basicConfig(filename="scraping_progress.log", level=logging.INFO,
#                     format='%(asctime)s - %(message)s')
# logger = logging.getLogger()

# # Base URLs (added pageNumber for first URL)
# base_url_1 = "http://77.42.251.205/LegisltaionSearch.aspx?searchText=&AndOr=AND&optionID=-1&status=-1&classification0&lawNumber=0&lawYear={year}&articleNumber=0&pageNumber={page_number}"
# base_url_2 = "http://77.42.251.205/AdvancedRulingSearch.aspx?searchText=&AndOr=AND&typeid=0&courtID=0&depid=0&rulNumber=0&rulYear={year}&judjes=&desicionmonth=0&DesicionDay=0&DesicionYear=0&&&pageNumber={page_number}&language=ar"

# # Directory to save the files
# output_dir = "Final_Laws"
# if not os.path.exists(output_dir):
#     os.makedirs(output_dir)

# def scrape_law_links_from_url_1(year):
#     page_number = 1  # Pagination starts at 1
#     all_law_links = []
#     logger.info(f"Scraping links from first URL for year {year}...")

#     while True:
#         url = base_url_1.format(year=year, page_number=page_number)
#         response = requests.get(url)
#         if response.status_code != 200:
#             logger.error(f"Failed to retrieve data for year {year} from the first URL. Status code: {response.status_code}.")
#             break

#         soup = BeautifulSoup(response.text, 'html.parser')
#         main_div = soup.find('div', {'id': 'MainContent_mainLegTr'})
#         if not main_div:
#             logger.info(f"No container div found for year {year} on page {page_number}.")
#             break

#         # Find all law links inside this div
#         law_links = []
#         for wrap in main_div.find_all('div', class_='extra-wrap'):
#             a_tag = wrap.find('a', href=True)
#             if a_tag:
#                 full_link = f"http://77.42.251.205/{a_tag['href'].lstrip('/')}"
#                 law_links.append(full_link)

#         if not law_links:
#             logger.info(f"No more law links found for year {year} on page {page_number}.")
#             break

#         all_law_links.extend(law_links)
#         logger.info(f"Scraped {len(law_links)} links from page {page_number} for year {year}.")

#         # Check if there is a next page
#         pager = soup.find('ul', {'id': 'MainContent_Pager1_ulPager'})
#         if pager:
#             active = pager.find('li', class_='active')
#             if active and active.find_next_sibling('li') and 'active' not in active.find_next_sibling('li').get('class', []):
#                 page_number += 1
#             else:
#                 break
#         else:
#             break

#         time.sleep(2)
#     return all_law_links


# def scrape_law_links_from_url_2(year):
#     page_number = 1  # Usually pagination starts at 1
#     all_law_links = []
#     logger.info(f"Scraping links from second URL for year {year}...")

#     while True:
#         url = base_url_2.format(year=year, page_number=page_number)
#         response = requests.get(url)
#         if response.status_code != 200:
#             logger.error(f"Failed to retrieve data for year {year} from the second URL. Status code: {response.status_code}.")
#             break

#         soup = BeautifulSoup(response.text, 'html.parser')
#         main_div = soup.find('div', {'id': 'MainContent_mainLegTr'})
#         if not main_div:
#             logger.info(f"No container div found for year {year} on page {page_number}.")
#             break

#         # Extract all links on this page
#         law_links = []
#         for wrap in main_div.find_all('div', class_='extra-wrap'):
#             a_tag = wrap.find('a', href=True)
#             if a_tag:
#                 full_link = f"http://77.42.251.205/{a_tag['href'].lstrip('/')}"
#                 law_links.append(full_link)

#         if not law_links:
#             logger.info(f"No more law links found for year {year} on page {page_number}. Ending pagination.")
#             break

#         all_law_links.extend(law_links)
#         logger.info(f"Scraped {len(law_links)} links from page {page_number} for year {year}.")

#         # Check if there is a next page by looking for the pager control
#         pager = soup.find('ul', {'id': 'MainContent_Pager1_ulPager'})
#         if pager:
#             # Find the current active page li
#             active_li = pager.find('li', class_='active')
#             if active_li:
#                 next_li = active_li.find_next_sibling('li')
#                 # If next_li exists and is not disabled, continue
#                 if next_li and ('disabled' not in next_li.get('class', [])):
#                     page_number += 1
#                 else:
#                     logger.info(f"No next page found for year {year} after page {page_number}.")
#                     break
#             else:
#                 # No active page found, stop to avoid infinite loop
#                 logger.info(f"No active page found in pager for year {year} on page {page_number}.")
#                 break
#         else:
#             # No pager found, assume single page
#             logger.info(f"No pager control found for year {year} on page {page_number}.")
#             break

#         time.sleep(2)

#     return all_law_links


# def save_links_to_file(year, links):
#     file_path = f"{output_dir}/{year}.txt"
#     with open(file_path, "w", encoding="utf-8") as file:
#         for link in links:
#             file.write(f"{link}\n")
#     logger.info(f"Saved {len(links)} links for year {year} to {file_path}")

# def scrape_all_years():
#     start_time = time.time()
#     for year in range(1900, 2024):
#         logger.info(f"Starting to scrape year {year}...")
#         all_law_links = []
#         law_links_url_1 = scrape_law_links_from_url_1(year)
#         all_law_links.extend(law_links_url_1)
#         law_links_url_2 = scrape_law_links_from_url_2(year)
#         all_law_links.extend(law_links_url_2)
#         if all_law_links:
#             save_links_to_file(year, all_law_links)
#         else:
#             logger.info(f"No links found for year {year}")
#         elapsed_time = time.time() - start_time
#         remaining_years = 2023 - year
#         avg_time_per_year = elapsed_time / (year - 1900 + 1)
#         remaining_time = avg_time_per_year * remaining_years
#         eta = datetime.fromtimestamp(time.time() + remaining_time)
#         logger.info(f"Scraped year {year} - ETA: {eta.strftime('%Y-%m-%d %H:%M:%S')}")

# if __name__ == "__main__":
#     scrape_all_years()
import requests
from bs4 import BeautifulSoup
import time
import os
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging
logging.basicConfig(filename="scraping_progress.log", level=logging.INFO,
                    format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# Base URLs (added pageNumber for first URL)
base_url_1 = "http://77.42.251.205/LegisltaionSearch.aspx?searchText=&AndOr=AND&optionID=-1&status=-1&classification0&lawNumber=0&lawYear={year}&articleNumber=0&pageNumber={page_number}"
base_url_2 = "http://77.42.251.205/AdvancedRulingSearch.aspx?searchText=&AndOr=AND&typeid=0&courtID=0&depid=0&rulNumber=0&rulYear={year}&judjes=&desicionmonth=0&DesicionDay=0&DesicionYear=0&&&pageNumber={page_number}&language=ar"

# Directory to save the files
output_dir = "Final_Laws"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

def scrape_law_links_from_url_1(year):
    page_number = 1  # Pagination starts at 1
    all_law_links = []
    logger.info(f"Scraping links from first URL for year {year}...")

    while True:
        url = base_url_1.format(year=year, page_number=page_number)
        response = requests.get(url)
        if response.status_code != 200:
            logger.error(f"Failed to retrieve data for year {year} from the first URL. Status code: {response.status_code}.")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        main_div = soup.find('div', {'id': 'MainContent_mainLegTr'})
        if not main_div:
            logger.info(f"No container div found for year {year} on page {page_number}.")
            break

        # Find all law links inside this div
        law_links = []
        for wrap in main_div.find_all('div', class_='extra-wrap'):
            a_tag = wrap.find('a', href=True)
            if a_tag:
                full_link = f"http://77.42.251.205/{a_tag['href'].lstrip('/')}"
                law_links.append(full_link)

        if not law_links:
            logger.info(f"No more law links found for year {year} on page {page_number}.")
            break

        all_law_links.extend(law_links)
        logger.info(f"Scraped {len(law_links)} links from page {page_number} for year {year}.")

        # Check if there is a next page
        pager = soup.find('ul', {'id': 'MainContent_Pager1_ulPager'})
        if pager:
            active = pager.find('li', class_='active')
            if active and active.find_next_sibling('li') and 'active' not in active.find_next_sibling('li').get('class', []):
                page_number += 1
            else:
                break
        else:
            break

        time.sleep(2)
    return all_law_links


def scrape_law_links_from_url_2(year):
    page_number = 1  # Usually pagination starts at 1
    all_law_links = []
    logger.info(f"Scraping links from second URL for year {year}...")

    while True:
        url = base_url_2.format(year=year, page_number=page_number)
        response = requests.get(url)
        if response.status_code != 200:
            logger.error(f"Failed to retrieve data for year {year} from the second URL. Status code: {response.status_code}.")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        main_div = soup.find('div', {'id': 'MainContent_mainLegTr'})
        if not main_div:
            logger.info(f"No container div found for year {year} on page {page_number}.")
            break

        # Extract all links on this page
        law_links = []
        for wrap in main_div.find_all('div', class_='extra-wrap'):
            a_tag = wrap.find('a', href=True)
            if a_tag:
                full_link = f"http://77.42.251.205/{a_tag['href'].lstrip('/')}"
                law_links.append(full_link)

        if not law_links:
            logger.info(f"No more law links found for year {year} on page {page_number}. Ending pagination.")
            break

        all_law_links.extend(law_links)
        logger.info(f"Scraped {len(law_links)} links from page {page_number} for year {year}.")

        # Check if there is a next page by looking for the pager control
        pager = soup.find('ul', {'id': 'MainContent_Pager1_ulPager'})
        if pager:
            # Find the current active page li
            active_li = pager.find('li', class_='active')
            if active_li:
                next_li = active_li.find_next_sibling('li')
                # If next_li exists and is not disabled, continue
                if next_li and ('disabled' not in next_li.get('class', [])):
                    page_number += 1
                else:
                    logger.info(f"No next page found for year {year} after page {page_number}.")
                    break
            else:
                # No active page found, stop to avoid infinite loop
                logger.info(f"No active page found in pager for year {year} on page {page_number}.")
                break
        else:
            # No pager found, assume single page
            logger.info(f"No pager control found for year {year} on page {page_number}.")
            break

        time.sleep(2)

    return all_law_links


def save_links_to_file(year, links):
    file_path = f"{output_dir}/{year}.txt"
    with open(file_path, "w", encoding="utf-8") as file:
        for link in links:
            file.write(f"{link}\n")
    logger.info(f"Saved {len(links)} links for year {year} to {file_path}")

def scrape_year(year):
    logger.info(f"Starting to scrape year {year}...")
    all_law_links = []
    law_links_url_1 = scrape_law_links_from_url_1(year)
    all_law_links.extend(law_links_url_1)
    law_links_url_2 = scrape_law_links_from_url_2(year)
    all_law_links.extend(law_links_url_2)
    if all_law_links:
        save_links_to_file(year, all_law_links)
    else:
        logger.info(f"No links found for year {year}")
    elapsed_time = time.time() - scrape_year.start_time
    remaining_years = 2023 - year
    avg_time_per_year = elapsed_time / (year - 1900 + 1)
    remaining_time = avg_time_per_year * remaining_years
    eta = datetime.fromtimestamp(time.time() + remaining_time)
    logger.info(f"Scraped year {year} - ETA: {eta.strftime('%Y-%m-%d %H:%M:%S')}")


def scrape_all_years_multithreaded(max_workers=5):
    scrape_year.start_time = time.time()
    years = list(range(1900, 2024))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all years to executor
        futures = [executor.submit(scrape_year, year) for year in years]

        # Wait for all to finish (order of completion can vary)
        for future in as_completed(futures):
            future.result()  # To raise exceptions if any

if __name__ == "__main__":
    scrape_all_years_multithreaded(max_workers=8)
