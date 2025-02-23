# ============================================================================
#
# File    : nyt_webscrap.py
# Date    : 2024/09/29
# (c)     : Michaël Abergel - Alfred Quitman - Emmanuel Bompard
# Object  : This module provides web scrapping capabilities.
# Version : 0.1.0
#
# ============================================================================

## Imports
#
import logging
import requests
from inspect import currentframe
import urllib.request

# Some soup (beautiful but no Campbell)
import bs4

class NYTWebScrap():
    
    @staticmethod
    def amazon_price(p_isbn10: str, *, p_country: str="FR") -> str:
        """
        Get the Amazon price of the book denoted by the ISBN-10 parameter.
            
        Parameters :
          - p_isbn10 : the ISBN10 number of the book to look for ;
          - p_country : the Amazon website to scan.
        """
        # Headers to fool Amazon (hopefully)
        _headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
        }

        # The (HTML) class to look for :
        # Note : should be in a configuration file
        _html_class = "a-size-base a-color-price a-color-price"
        
        # Get the right site
        match p_country:
            case "FR":
                _base_url = "https://www.amazon.fr"
            case "IT":
                _base_url = "https://www.amazon.it"
            case "DE":
                _base_url = "https://www.amazon.de"
            case "JP":
                _base_url = "https://www.amazon.co.jp"
            case "UK":
                _base_url = "https://www.amazon.co.uk"
            case "US":
                _base_url = "https://www.amazon.com"
            case _:
                _base_url = "https://www.amazon.fr"

        _full_url = f"{_base_url}/dp/{p_isbn10}"
        logging.info(f"{currentframe().f_code.co_name} : {_full_url = }")
        
        try:
            # Create a request with URL and headers
            _req = urllib.request.Request(_full_url, headers=_headers)

            # Get the page
            with urllib.request.urlopen(_req) as _reply:
                _page = _reply.read()
            
            # Analyse it
            _soup = bs4.BeautifulSoup(_page, "html.parser")
            
            # Reach for the (hopefully) right field
            _item_price_span = list(_soup.findAll("span", attrs={"class": _html_class}))[0]
            
            # Extract the price
            _item_price = _item_price_span.get_text(strip=True).replace("\xa0", " ")
        except Exception as _e:
            logging.error(f"{currentframe().f_code.co_name} : something went wrong {_e = }")
            _item_price = "unknown price"
        
        return _item_price
