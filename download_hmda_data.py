# Import required libraries
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from pathlib import Path
import logging
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager  # Automates driver management
from selenium.webdriver.chrome.options import Options
import re
from email.utils import parsedate_to_datetime
from datetime import datetime

# Determine subfolder for an HMDA filename
def determine_raw_subfolder(file_name: str) -> str:
    """
    Route downloaded ZIP files into subfolders under data/raw based on filename.

    Returns one of: 'loans', 'msamd', 'panel', 'transmissal_series', 'misc'.
    """
    name = file_name.lower()

    # MSAMD geography files.
    if 'msamd' in name:
        return 'msamd'

    # Transmittal Series (TS).
    if ('public_ts' in name) or ('transmiss' in name) or re.search(r'(^|[_-])ts([_-]|$)', name):
        return 'transmissal_series'

    # Panel files.
    if ('public_panel' in name) or re.search(r'(^|[_-])panel([_-]|$)', name):
        return 'panel'

    # Loan files (LAR/MLAR/nationwide loan-level).
    if ('mlar' in name) or ('public_lar' in name) or re.search(r'(^|[_-])lar([_-]|$)', name) or ('nationwide' in name):
        return 'loans'

    # Other miscellaneous assets.
    if ('arid2017' in name) or ('avery' in name):
        return 'misc'

    # Default catch-all.
    return 'misc'

# Download ZIP files from a URL
def download_zip_files_from_url(
    page_url: str,
    destination_folder: str,
    pause_length: int=5,
    wait_time: int=10,
    download_csvs: bool=True,
    download_pipes: bool=False,
    download_all: bool=False,
    overwrite_mode: str='skip',
):
    """
    Find all ZIP links on a webpage (after JavaScript rendering) and download
    them to a specified folder.

    Note: Selenium is used because the requests package returns only a blank
    HTML template for HMDA pages.

    Args:
        page_url (str): URL to scrape for ZIP links.
        destination_folder (str): Folder where ZIP files will be downloaded.
        pause_length (int): Seconds to pause between downloads.
        wait_time (int): Seconds to wait for JavaScript to load.
        overwrite_mode (str): Behavior if the destination file exists.
            - 'skip' (default): do not re-download
            - 'always': always re-download and overwrite
            - 'if_newer': re-download if server Last-Modified is newer than local mtime
            - 'if_size_diff': re-download if server Content-Length differs from local size
    """

    try:

        # Ensure the destination folder exists.
        dest_path = Path(destination_folder)
        dest_path.mkdir(parents=True, exist_ok=True)

        # Set up the Selenium WebDriver.
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run Chrome in headless mode.
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Use webdriver-manager to download and manage the ChromeDriver.
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

        logging.info(f"Fetching content from URL with Selenium: {page_url}")
        driver.get(page_url)
        
        # Wait for JavaScript to load content.
        logging.info(f"Waiting {wait_time} seconds for JavaScript to load...")
        time.sleep(wait_time)

        page_source = driver.page_source
        driver.quit()

        # Parse the HTML page.
        soup = BeautifulSoup(page_source, 'html.parser')

        # Find all <a> tags with an href attribute.
        zip_links_found = 0
        for link_tag in soup.find_all('a', href=True):
            href = link_tag['href']
            
            # Check if the link points to a ZIP file.
            zip_extensions = ('.zip',)
            if href.lower().endswith(zip_extensions) & ((('csv' in href.lower()) & download_csvs) | (('pipe' in href.lower()) & download_pipes) | download_all) :
                zip_links_found += 1

                # Construct the full URL (handles relative links).
                file_url = urljoin(page_url, href)
                
                # Extract a clean filename from the URL.
                try:
                    file_name = Path(urlparse(file_url).path).name
                    if not file_name:  # Handle cases where the path ends with /
                        file_name = f"downloaded_zip_{zip_links_found}{Path(file_url).suffix}"
                except Exception as e:
                    logging.warning(f"Could not derive filename from URL {file_url}: {e}. Using a generic name.")
                    file_name = f"zip_file_{zip_links_found}{Path(href).suffix or '.zip'}"

                # Determine destination subfolder based on the file name.
                subfolder = determine_raw_subfolder(file_name)
                subfolder_path = dest_path / subfolder
                subfolder_path.mkdir(parents=True, exist_ok=True)

                # Download new files or overwrite existing ones based on mode.
                file_path = subfolder_path / file_name
                need_download = not os.path.exists(file_path)

                if not need_download and overwrite_mode.lower() in ['always']:
                    need_download = True

                if not need_download and overwrite_mode.lower() in ['if_newer', 'if_size_diff']:
                    try:
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
                        }
                        head_resp = requests.head(file_url, headers=headers, allow_redirects=True, timeout=30)
                        head_resp.raise_for_status()

                        if overwrite_mode.lower() == 'if_newer':
                            last_mod = head_resp.headers.get('Last-Modified')
                            if last_mod:
                                try:
                                    remote_dt = parsedate_to_datetime(last_mod)
                                    local_dt = datetime.utcfromtimestamp(os.path.getmtime(file_path))
                                    if remote_dt.tzinfo is None:
                                        remote_dt = remote_dt.replace(tzinfo=None)
                                    # Compare in UTC naive format.
                                    if remote_dt > local_dt:
                                        need_download = True
                                except Exception as e:
                                    logging.warning(f"Could not parse Last-Modified for {file_name}: {e}")
                        if (not need_download) and (overwrite_mode.lower() == 'if_size_diff'):
                            cl = head_resp.headers.get('Content-Length')
                            if cl is not None:
                                try:
                                    remote_size = int(cl)
                                    local_size = os.path.getsize(file_path)
                                    if remote_size != local_size:
                                        need_download = True
                                except Exception as e:
                                    logging.warning(f"Size compare failed for {file_name}: {e}")
                    except requests.exceptions.RequestException as e:
                        logging.warning(f"HEAD request failed for {file_url}: {e}. Proceeding per overwrite_mode='{overwrite_mode}'.")

                if need_download:
                    logging.info(f"Downloading {file_url} to {file_path}...")
                    try:
                        # Specify a user agent for the download request.
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
                        }
                        # Stream file content to the local file.
                        file_response = requests.get(file_url, headers=headers, stream=True, timeout=60)
                        file_response.raise_for_status()
                        with open(file_path, 'wb') as f:
                            for chunk in file_response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        
                        logging.info(f"Successfully downloaded {file_name}")

                        # Pause between downloads.
                        time.sleep(pause_length)

                    except requests.exceptions.RequestException as e:
                        logging.error(f"Error downloading {file_url}: {e}")
                    except IOError as e:
                        logging.error(f"Error saving file {file_name} to {file_path}: {e}")
                else:
                    logging.info(f"File {file_path} already exists. Skipping download (overwrite_mode='{overwrite_mode}').")
        
        if zip_links_found == 0:
            logging.info("No ZIP links found on the page.")

    except Exception as e:  # Catch WebDriver and other general errors.
        logging.error(f"An unexpected error occurred: {e}")

# Main routine
if __name__ == "__main__":

    # Base URLs.
    mlar_base_url = 'https://ffiec.cfpb.gov/data-publication/modified-lar'
    snapshot_base_url = 'https://ffiec.cfpb.gov/data-publication/snapshot-national-loan-level-dataset'
    one_year_base_url = 'https://ffiec.cfpb.gov/data-publication/one-year-national-loan-level-dataset'
    three_year_base_url = 'https://ffiec.cfpb.gov/data-publication/three-year-national-loan-level-dataset'
    historical_url = 'https://www.consumerfinance.gov/data-research/hmda/historic-data/?geo=nationwide&records=all-records&field_descriptions=codes'

    # Download parameters.
    min_static_year = 2024
    max_static_year = 2024
    download_folder = "./data/raw"  # Destination folder for downloads.

    # Download static files.
    for year in range(min_static_year, max_static_year+1) :
        for base_url in [snapshot_base_url, one_year_base_url, three_year_base_url] :
            target_url = base_url + f'/{year}'
            download_zip_files_from_url(target_url, download_folder)

    # Download MLAR files (currently excludes headers).
    for year in range(2018, 2024+1) :
        target_url = mlar_base_url + f'/{year}'
        download_zip_files_from_url(target_url, download_folder, download_all=True)

    # Download historical files (2007–2017).
    target_url = historical_url
    download_zip_files_from_url(target_url, download_folder, download_all=True)
