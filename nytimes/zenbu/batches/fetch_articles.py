# fetch_articles.py

import os
import json
import time
import requests
import logging
from urllib.parse import urljoin
from dotenv import load_dotenv
import argparse
from datetime import datetime
import logging

# Load environment variables
load_dotenv()

# Ensure the logs directory exists
log_directory = os.path.dirname("../logs/fetch_articles.log")
os.makedirs(log_directory, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/fetch_articles.log"),
        logging.StreamHandler()
    ]
)

# Constants
API_KEY = os.getenv("KEY_API")
BASE_URI = os.getenv("BASE_URI")
ARCHIVE_FILENAME_PREFIX = os.getenv("ARCHIVE_FILENAME_PREFIX", "archive")
ARCHIVE_FILENAME_SUFFIX = os.getenv("ARCHIVE_FILENAME_SUFFIX", ".json")
STATE_FILE = "state.json"
ARTICLES_OUTPUT_FILE = os.getenv("ARTICLES_OUTPUT_FILE")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "../exports")
DELAY = 1  # Delay in seconds between requests
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "../completed/processed_files")
ARTICLES_FILENAME_PREFIX = os.getenv("ARTICLES_FILENAME_PREFIX", "articles_search")
ARTICLES_FILENAME_SUFFIX = os.getenv("ARTICLES_FILENAME_SUFFIX", ".json")
MONTHS_TO_FETCH = 36  # Number of months to fetch archives

# Template for archive filenames
ARCHIVE_OUTPUT_FILE = os.path.join(
    OUTPUT_DIR, f"{ARCHIVE_FILENAME_PREFIX}_{{year}}_{{month:02d}}{ARCHIVE_FILENAME_SUFFIX}"
)

# Ensure the export directory exists
directory = os.path.dirname(ARTICLES_OUTPUT_FILE)
os.makedirs(directory, exist_ok=True)

# # Class for retrieving archives
class NYTimesArchiveFetcher:

    """
        Initializes the NYTimesArchiveFetcher object with necessary parameters.

        Args:
            api_key (str): API key for authentication.
            base_uri (str): Base URI for the NYT API.
            output_file (str): Path to the output file where data will be saved.
            delay (int): Delay in seconds between requests (default: 1 second).
        """
    
    def __init__(self, api_key, base_uri, output_file, delay=1):
        self.api_key = api_key
        self.base_uri = base_uri
        self.output_file = output_file
        self.delay = delay

    def construct_url(self, year, month):

        """
        Constructs the URL for fetching archive data for a specific year and month.

        Args:
            year (int): The year for the archive.
            month (int): The month for the archive.

        Returns:
            str: The full URL for the archive request.
        """

        url_path = f"/svc/archive/v1/{year}/{month}.json"
        logging.debug(f"URL construite : {url_path}")
        return urljoin(self.base_uri, url_path)

    def fetch_archive(self, url, year, month):

        """
        Fetches the archive data from the NYT API for a specific year and month.

        Args:
            url (str): The URL for the archive request.
            year (int): The year for the archive.
            month (int): The month for the archive.

        Returns:
            dict: The JSON response from the API, or None if the file already exists.
        """

         # Ensure the directory exists
        directory = os.path.dirname(self.output_file)
        os.makedirs(directory, exist_ok=True)
        # Construct the filename for the archive
        filename = os.path.join(directory, f'archive_{year}_{month:02d}.json')
          # Check if the file already exists
        if os.path.exists(filename):
            logging.info(f"Le fichier {filename} existe déjà. Skipping download.")
            return None

        params = {"api-key": self.api_key}
        logging.info(f"Récupération des données depuis l'URL : {url}")
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            logging.info("Données récupérées avec succès.")
            time.sleep(self.delay)
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Erreur lors de la récupération des données : {e}")
            raise

    def save_json(self, data, year, month):

        """
        Saves the fetched JSON data to a file.

        Args:
            data (dict): The JSON data to save.
            year (int): The year associated with the data.
            month (int): The month associated with the data.
        """

        directory = os.path.dirname(self.output_file)
        os.makedirs(directory, exist_ok=True)
    
    # Save the file directly with the final intended name
        final_filename = os.path.join(directory, f'archive_{year}_{month:02d}.json')
    
        with open(final_filename, 'w') as f:
            json.dump(data, f, indent=4)
    
        logging.info(f"Data saved in {final_filename}")


# Class for article search
class ArticlesSearchAPI:

    """
        Initializes the ArticlesSearchAPI object with necessary parameters.

        Args:
            api_key (str): API key for authentication.
            base_uri (str): Base URI for the NYT API.
            output_file (str): Path to the output file where search results will be saved.
            delay (int): Delay in seconds between requests (default: 1 second).
        """
    
    def __init__(self, api_key, base_uri, output_file, delay=1):
        self.api_key = api_key
        self.base_uri = base_uri
        self.output_file = output_file
        self.delay = delay

    def construct_url(self):
         
        """
        Constructs the URL for the article search API.

        Returns:
            str: The full URL for the article search request.
        """
         
        url_path = f"/svc/search/v2/articlesearch.json"
        return urljoin(self.base_uri, url_path)

    def fetch_articles_search(self):
        url = self.construct_url()
        params = {"api-key": self.api_key}
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            time.sleep(self.delay)
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error retrieving articles : {e}")
            raise

    def save_articles_search_to_json(self, data):

        """
        Saves the article search results to a JSON file.

        Args:
            data (dict): The JSON data to save.
        """

        directory = os.path.dirname(self.output_file)
        os.makedirs(directory, exist_ok=True)
        with open(self.output_file, 'w') as f:
            json.dump(data, f, indent=4)
        logging.info(f"Articles search data saved to {self.output_file}")

def load_state():
    """
    Loads the current state from a state file.

    Returns:
        dict: The state data containing the last processed year, month, and index.
    """
    
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            file_content = f.read().strip()
            if not file_content:
                # If the file is empty, return a default state
                return {"year": None, "month": None, "index": 0}
            
            try:
                return json.loads(file_content)
            except json.JSONDecodeError:
                # Log error if JSON is invalid
                logging.error(f"Error decoding JSON from {STATE_FILE}. Initializing default state.")
                return {"year": None, "month": None, "index": 0}
    
    # If the file does not exist, return default state
    return {"year": None, "month": None, "index": 0}

def save_state(state):
    """
    Saves the current state to a state file.

    Args:
        state (dict): The state data to save.
    """
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)  # Use indent for readability

def fetch_and_save_archive():
    """
    Fetches and saves the archives for the last 36 months.
    This function loads the current state, processes the year and month pairs,
    and fetches the corresponding archives. 
    It updates the state after successfully saving the archives.
    Returns:
        bool: True if there are more archives to fetch, False otherwise.
    """
    # Load the current state
    state = load_state()
    index = state.get("index")

    # Generate year-month pairs for the last 36 months starting from the current date
    year_month_pairs = []
    current_date = datetime.now()

    for _ in range(MONTHS_TO_FETCH):
        year_month_pairs.append((current_date.year, current_date.month))
        # Move to the previous month
        if current_date.month == 1:  # If January, go to December of the previous year
            current_date = current_date.replace(year=current_date.year - 1, month=12)
        else:
            current_date = current_date.replace(month=current_date.month - 1)

    # Check if all months and years have been processed
    if index >= len(year_month_pairs):
        logging.info("All months and years have been processed.")
        return False  # No more archives to fetch

    # Get the current year and month from the year-month pairs
    year, month = year_month_pairs[index]

    # Generate the output file name based on the current year and month
    ARCHIVE_OUTPUT_FILE = os.path.join(OUTPUT_DIR, f'archive_{year}_{month:02d}.json')
    PROCESSED_FILE = os.path.join(PROCESSED_DIR, f'archive_{year}_{month:02d}.json')

    # Log the output file being used
    logging.info(f"Checking archive files for {year}-{month}.")

    # Check if the file already exists in the Completed/processed directory
    if os.path.exists(PROCESSED_FILE):
        logging.info(f"The file {PROCESSED_FILE} already exists in {PROCESSED_DIR}. Skipping download.")
        # Update the state to move to the next file
        state["index"] += 1
        save_state(state)
        return True  # More archives to fetch

    # If not found in Completed/processed, check if it exists in the exports directory
    if os.path.exists(ARCHIVE_OUTPUT_FILE):
        logging.info(f"The file {ARCHIVE_OUTPUT_FILE} already exists in {OUTPUT_DIR}. Nothing to process.")
        state["index"] += 1  # Move to the next index
        save_state(state)
        return True  # More archives to fetch

    # If the file doesn't exist in either directory, proceed to fetch the archive
    logging.info(f"The file for {year}-{month} does not exist in {OUTPUT_DIR} or {PROCESSED_DIR}. Fetching data...")

    # Instantiate the fetcher with the appropriate output file
    fetcher = NYTimesArchiveFetcher(API_KEY, BASE_URI, ARCHIVE_OUTPUT_FILE, delay=DELAY)
    
    # Construct the URL for the archive
    url = fetcher.construct_url(year, month)

    try:
        # Fetch the archive data
        archive_data = fetcher.fetch_archive(url, year, month)
        if archive_data:
            # Save the archive data
            fetcher.save_json(archive_data, year, month)
            logging.info(f"Archives for {year}-{month} saved.")
        
        # Update the state
        state["year"] = year
        state["month"] = month

    except Exception as e:
        logging.error(f"Failed to fetch or save archives for {year}-{month}: {e}")

    # After fetching and saving:
    # Update the state to move to the next index
    state["index"] += 1
    save_state(state)
    
    return True  # More archives to fetch


def move_file_to_processed(source_file, destination_dir):

    # Extract the filename from the source path
    filename = os.path.basename(source_file)

    # Construct the full path for the destination file
    destination_file = os.path.join(destination_dir, filename)
    
    logging.info(f"Moving file from '{source_file}' to '{destination_file}'")

    # Move the file if it exists in the source directory and not in the destination
    os.rename(source_file, destination_file)
    logging.info(f"Moved {source_file} to {destination_file}")
    
def fetch_and_save_articles():
    """
    Fetches and saves article search results.
    This function interacts with the article search API, fetches the results, and saves them to a JSON file.
    """
    # Define processed file path
    PROCESSED_FILE = os.path.join(PROCESSED_DIR, "article_search.json")

    # Check if the file already exists in the processed directory
    if os.path.exists(PROCESSED_FILE):
        logging.info(f"The file {PROCESSED_FILE} already exists. Skipping download.")
        return

    articles_api = ArticlesSearchAPI(API_KEY, BASE_URI, ARTICLES_OUTPUT_FILE, delay=DELAY)
    try:
        articles_data = articles_api.fetch_articles_search()
        articles_api.save_articles_search_to_json(articles_data)

        # Move the file to the processed directory after saving
        #move_file_to_processed(ARTICLES_OUTPUT_FILE, PROCESSED_DIR)
    except Exception as e:
        logging.error(f"Error while saving articles: {e}")

def cron_fetch_new_articles():
    """
    Cron task function to fetch new articles from the NYT API.
    This function can be scheduled to run at regular intervals.
    """
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f"Starting the cron task to fetch new articles at {current_time}.")
    try:
        fetch_and_save_articles()  # Call the function to fetch articles
        logging.info("Finished fetching new articles.")
    except Exception as e:
        logging.error(f"Error during fetching new articles: {e}")

def main():
    logging.info("Démarrage de la récupération des archives")
    
    # Charger l'état actuel
    state = load_state()
    
    # Log l'état actuel pour le débogage
    logging.info(f"État actuel chargé : {state}")

    # Vérifier si tous les mois ont été traités
    if state.get("index", 0) >= MONTHS_TO_FETCH:
        logging.info("Tous les mois ont été traités. Réinitialisation de l'état.")
        state["index"] = 0  # Réinitialiser l'index à 0
        save_state(state)    # Sauvegarder le nouvel état

    # Appeler la fonction pour récupérer et sauvegarder les archives
    more_archives = fetch_and_save_archive()
    
    logging.info("Récupération des archives terminée")
    if not more_archives:
        logging.info("Aucune archive à traiter.")

if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='NYTimes Article Fetcher')
    parser.add_argument('--cron', action='store_true', help='Run cron job for fetching new articles')
    args = parser.parse_args()

    if args.cron:
        cron_fetch_new_articles()  # Run the cron task if --cron is passed
    else:
        main()  # Otherwise, run the normal main process