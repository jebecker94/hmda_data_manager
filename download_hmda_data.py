# Import Packages
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from pathlib import Path
import logging
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager # Automates driver management
from selenium.webdriver.chrome.options import Options

# Download Excel Files from URL
def download_zip_files_from_url(
    page_url: str,
    destination_folder: str,
    pause_length: int=5,
    wait_time: int=10,
    download_csvs: bool=True,
    download_pipes: bool=False,
    download_all: bool=False,
):
    """
    Finds all linked ZIP files on a given webpage (after JavaScript rendering)
    and downloads them to a specified folder.

    Note: This function uses Selenium's web driver because otherwise the requests
    package will return a blank HTML template, rather than the full HMDA web pages.

    Args:
        page_url (str): The URL of the webpage to scrape for ZIP links.
        destination_folder (str): The path to the folder where ZIP files
                                  will be downloaded.
        pause_length (int): Time in seconds to pause between downloads.
        wait_time (int): Time in seconds to wait for the page to load JavaScript.
    """

    try:

        # Ensure the destination folder exists
        dest_path = Path(destination_folder)
        dest_path.mkdir(parents=True, exist_ok=True)

        # Setup Selenium WebDriver
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run Chrome in headless mode (no GUI)
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Use webdriver-manager to automatically download and manage the ChromeDriver
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

        logging.info(f"Fetching content from URL with Selenium: {page_url}")
        driver.get(page_url)
        
        # Wait for JavaScript to load content
        logging.info(f"Waiting {wait_time} seconds for JavaScript to load...")
        time.sleep(wait_time)

        page_source = driver.page_source
        driver.quit()

        # Parse the HTML Page
        soup = BeautifulSoup(page_source, 'html.parser')

        # Find all <a> tags with an href attribute
        zip_links_found = 0
        for link_tag in soup.find_all('a', href=True):
            href = link_tag['href']
            
            # Check if the link points to a ZIP file
            zip_extensions = ('.zip',)
            if href.lower().endswith(zip_extensions) & ((('csv' in href.lower()) & download_csvs) | (('pipe' in href.lower()) & download_pipes) | download_all) :
                zip_links_found += 1

                # Construct the full URL (handles relative links)
                file_url = urljoin(page_url, href)
                
                # Extract a clean filename from the URL
                try:
                    file_name = Path(urlparse(file_url).path).name
                    if not file_name: # Handle cases where path might end in /
                        file_name = f"downloaded_zip_{zip_links_found}{Path(file_url).suffix}"
                except Exception as e:
                    logging.warning(f"Could not derive filename from URL {file_url}: {e}. Using a generic name.")
                    file_name = f"zip_file_{zip_links_found}{Path(href).suffix or '.zip'}"

                # Only Download New Files
                file_path = dest_path / file_name
                if not os.path.exists(file_path) :
                    logging.info(f"Downloading {file_url} to {file_path}...")
                    try:
                        # Specify User Agent for getting page contents (for the download request)
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
                        }
                        # Stream File Content to Local File
                        file_response = requests.get(file_url, headers=headers, stream=True, timeout=60)
                        file_response.raise_for_status()
                        with open(file_path, 'wb') as f:
                            for chunk in file_response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        
                        logging.info(f"Successfully downloaded {file_name}")

                        # Courtesy Pause
                        time.sleep(pause_length)

                    except requests.exceptions.RequestException as e:
                        logging.error(f"Error downloading {file_url}: {e}")
                    except IOError as e:
                        logging.error(f"Error saving file {file_name} to {file_path}: {e}")
                else:
                    logging.info(f"File {file_path} already exists. Skipping download.")
        
        if zip_links_found == 0:
            logging.info("No zip file links found on the page.")

    except Exception as e: # Catch WebDriver exceptions and other general errors
        logging.error(f"An unexpected error occurred: {e}")

# Main Routine
if __name__ == "__main__":

    # Base URLs
    mlar_base_url = 'https://ffiec.cfpb.gov/data-publication/modified-lar'
    snapshot_base_url = 'https://ffiec.cfpb.gov/data-publication/snapshot-national-loan-level-dataset'
    one_year_base_url = 'https://ffiec.cfpb.gov/data-publication/one-year-national-loan-level-dataset'
    three_year_base_url = 'https://ffiec.cfpb.gov/data-publication/three-year-national-loan-level-dataset'
    historical_url = 'https://www.consumerfinance.gov/data-research/hmda/historic-data/?geo=nationwide&records=originated-records&field_descriptions=codes'

    # Download Parameters
    min_static_year = 2017
    max_static_year = 2023
    download_folder = "./data/raw/hmda_temp" # Changed folder to be more specific

    # Download Static Files
    for year in range(min_static_year, max_static_year+1) :
        for base_url in [snapshot_base_url, one_year_base_url, three_year_base_url] :
            target_url = base_url + f'/{year}'
            download_zip_files_from_url(target_url, download_folder)

    # Download MLAR Files (Currently Doesn't Include Headers)
    for year in range(2018, 2024+1) :
        target_url = mlar_base_url + f'/{year}'
        download_zip_files_from_url(target_url, download_folder, download_all=True)

    # Download Historical Files (2007-2017)
    target_url = historical_url
    download_zip_files_from_url(target_url, download_folder, download_all=True)
