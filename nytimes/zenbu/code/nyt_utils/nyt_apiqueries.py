# ============================================================================
#
# File    : nyt_apiqueries.py
# Date    : 2024/09/07
# (c)     : Michaël Abergel - Alfred Quitman - Emmanuel Bompard
# Object  : This is the interface between project programs and project API.
#           Hence defines a class of methods calling the project API and
#           returning the results ready to be used by other applications.
# Version : 0.2.0
#
# ============================================================================

## Imports
#
import logging
import json

from requests.auth import HTTPBasicAuth
import requests

class NYTAPIQueries():
    
    def __init__(self, p_d_api: dict) -> None :
        """
        The constructor.
        
        Parameters :
          - p_d_api : the dictionary describing the API
        """
        self.d_api = p_d_api.copy()

        return

    def build_url(self, p_endpoint: str) -> str:
        """
        Build an URL based on the API definition dictionary and endpoint.
        """
        return "{}://{}:{}{}".format(
                self.d_api["api_protocol"],
                self.d_api["api_address"],
                self.d_api["api_port"],
                self.d_api["api_endpoints"][p_endpoint],
            )    

    def get_price(self, p_isbn10: str, p_country: str):
        """
        Call the price API for an ISBN10.
        
        The endpoint is known by configuration. 
        
        However, like other endpoints defined, their parameters are
        not defined in the global YAML (ideally, some configuration
        would be appreciated too).
        
        Parameters :
          - p_isbn10 : the ISBN10 to evaluate
          - p_country : the code of the country web site to request
        
        Return : 
          - the price as a string (with its currency)
        """
        # Build the URL
        _url = self.build_url("book_price")
        
        # Get the authentication object
        _auth_basic = HTTPBasicAuth(
            self.d_api["api_username"],
            self.d_api["api_password"]
        )
        
        # Specify the headers
        _headers = {'Content-Type': 'application/json'}
        
        # Parameters
        _params = {
            "isbn10": p_isbn10,
            "country": p_country,
        }
        
        # Send the request with parameters and authentication
        _req_res = requests.post(
            url = _url,
            auth = _auth_basic,
            headers = _headers,
            data = json.dumps(_params),
        )
        
        # Returns the result
        _d_res = _req_res.json()
        
        return _d_res["result"] if "result" in _d_res else "N/A"

