# fetch_books.py

import os
import json
import requests
import datetime
import time
import logging
from dotenv import load_dotenv
from urllib.parse import urljoin

# Load environment variables
load_dotenv()

# Ensure the logs directory exists
log_directory = os.path.dirname("../logs/fetch_books.log")
os.makedirs(log_directory, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_directory, "../logs/fetch_books.log")),
        logging.StreamHandler()
    ]
)

# Constants
API_KEY = os.getenv("KEY_API")
BASE_URI = os.getenv("BASE_URI")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "../exports")
REQUEST_DELAY = 12  # Delay in seconds between requests
BOOKS_FILENAME_PREFIX = os.getenv("ARCHIVE_FILENAME_PREFIX", "books")
BOOKS_FILENAME_SUFFIX = os.getenv("ARCHIVE_FILENAME_SUFFIX", ".json")

# BooksSearchAPI class handles book search operations
class BooksSearchAPI:
    def __init__(self, api_key, base_uri, delay=REQUEST_DELAY):
        """
        Initialize the BooksSearchAPI with API key, base URI, and request delay.

        Args:
            api_key (str): The API key for authenticating requests.
            base_uri (str): The base URI for the API.
            delay (int): Delay in seconds between requests to avoid rate limiting.
        """
        if not api_key or not base_uri:
            raise ValueError("KEY_API and BASE_URI must be provided in environment variables.")
        
        self.api_key = api_key
        self.base_uri = base_uri
        self.delay = delay

        logging.info(f"Initialized BooksSearchAPI with API_KEY: {self.api_key} and BASE_URI: {self.base_uri}")

    def construct_url(self, endpoint="svc/books/v3/lists/full-overview.json"):
        """
        Construct the full URL for the API request.

        Args:
            endpoint (str): The specific API endpoint to append to the base URI.

        Returns:
            str: The full URL constructed.
        """
        url = urljoin(self.base_uri, endpoint)
        logging.debug(f"Constructed URL: {url}")
        return url
    
    def fetch_books_for_date(self, date):
        """
        Fetch the book data from the API for a specific date.

        Args:
            date (str): The date for which to fetch book data (format: 'YYYY-MM-DD').

        Returns:
            dict: The JSON response from the API containing the book data.
        """
        params = {'api-key': self.api_key, 'published_date': date}
        url = self.construct_url()
        logging.debug(f"Fetching books for date {date} with URL: {url} and Params: {params}")
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for {date}: {e}")
            return None

    def get_sundays_between(self, start_date, end_date=None):
        """
        Get a list of all Sundays between two dates.

        Args:
            start_date (datetime.date): The start date.
            end_date (datetime.date): The end date (defaults to today if not provided).

        Returns:
            list: A list of dates (in 'YYYY-MM-DD' format) that fall on Sundays.
        """
        if end_date is None:
            end_date = datetime.date.today()

        sundays = []
        current_date = start_date + datetime.timedelta(days=(6 - start_date.weekday()))  # First Sunday

        while current_date <= end_date:
            sundays.append(current_date.strftime("%Y-%m-%d"))
            current_date += datetime.timedelta(weeks=1)  # Next Sunday

        logging.info(f"Calculated Sundays between {start_date} and {end_date}: {sundays}")
        return sundays

    def fetch_books_for_sundays(self, start_date, end_date=None):
        """
        Fetch and save book data for all Sundays between two dates.

        Args:
            start_date (datetime.date): The start date.
            end_date (datetime.date): The end date (defaults to today if not provided).
        """
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        sundays = self.get_sundays_between(start_date, end_date)

        for sunday in sundays:
            data = self.fetch_books_for_date(sunday)
            if data:
                file_name = f'books_{sunday}.json'
                file_path = os.path.join(OUTPUT_DIR, file_name)

                with open(file_path, 'w') as file:
                    json.dump(data, file)
                logging.info(f"Data for {sunday} saved to {file_path}")
            
            time.sleep(self.delay)
            logging.debug(f"Sleeping for {self.delay} seconds")
    
if __name__ == "__main__":
    START_DATE = datetime.date(2022, 7, 1)
    END_DATE = datetime.date(2024, 9, 10)
    
    api = BooksSearchAPI(API_KEY, BASE_URI)
    api.fetch_books_for_sundays(START_DATE, END_DATE)
