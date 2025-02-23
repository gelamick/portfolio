# ============================================================================
#
# File    : price_NYT_db.py
# Date    : 2024/11/16
# (c)     : Michaël Abergel - Alfred Quitman - Emmanuel Bompard
# Object  : Feed the price collection.
# Version : 0.1.0
#
# ============================================================================


##
# Basic imports (Python Library)
#
import os
import time
from datetime import datetime, timedelta
import glob
import logging
from functools import reduce
import json

##
# MongoDB, Pandas YAML etc.
from pymongo import MongoClient
import pandas as pd
import numpy as np
import yaml

##
# Project imports
from nyt_utils.nyt_script import NYTErr, NYTScript
from nyt_utils.nyt_apiqueries import NYTAPIQueries
from nyt_utils.nyt_dbqueries import NYTDBQueries

# ============================================================================

def main():
    """
    """
    # Instanciate a new script
    script = NYTScript(os.environ["NYT_CONFIG_FILE"])
    
    # Then, let's check whether a similar script is already running
    script.test_and_lock()
    
    # Errors set (for now not used as such but next sprint it might be...)
    _err_set = set()
    _err = NYTErr.ERR_OK
    
    # We wait for some time, just to let the API start nicely
    if (_delay := script.d_config["prices"]["delayed_launch"]) > 0:
        time.sleep(_delay)
    
    # Instanciate an API interface
    _nyt_api = NYTAPIQueries(script.d_config["api_calls"])
    
    # The method is quite simple (too simple, mayhap...)
    # - first we get the list of all ISBN10
    # - then we get the list of all priced ISBN10
    # - set a list of ISBN10 still to be priced
    # - for all items in the list get the price and insert in the Prices collection
    #
    # We may want to work with batches of ISBN but, for now we start this way, 
    # especially because it is not possible to call the API for a batch of items.
    # So...
    
    # We suppose we keep going indefinitely.
    _keep_going = True
    
    # We will wait between two requests because one can fear to be blacklisted by
    # the vendor (A..z.n)
    _waiting_for = script.d_config["prices"]["waiting_for"]
    
    # We will also wait between batches, to avoid querying the DB uselessly
    _global_waiting_for = script.d_config["prices"]["global_waiting_for"]
    
    # The used collections :
    _books_coll = script.db[script.d_config["collections"]["books"]["coll_name"]]
    _prices_coll = script.db[script.d_config["prices"]["coll_name"]]
    
    while _keep_going:
        # Get the oldest date (if the script runs 24/7 the date will evolve)
        _today = datetime.now().strftime("%Y-%m-%d")
        _oldest = (datetime.now() - timedelta(days=script.d_config["prices"]["validity"])).strftime("%Y-%m-%d")
        
        # Get the list of all ISBN
        _l_all_isbn = NYTDBQueries.books_all_isbn10(_books_coll)
        
        # Get the list of all priced ISBN10
        _l_all_isbn_prices = NYTDBQueries.prices_all_isbn(
            _prices_coll, 
            script.d_config["prices"]["country_code"], 
            _oldest,
        )
        
        # Hence get the list of items to be priced
        _l_to_do = list(set(_l_all_isbn) - set(_l_all_isbn_prices))
        
        for _isbn10 in _l_to_do:
            # Get the price
            _price = _nyt_api.get_price(_isbn10, script.d_config["prices"]["country_code"])
            
            # Upsert into the prices collection
            NYTDBQueries.prices_update_price(
                _prices_coll, _isbn10, _price,
                script.d_config["prices"]["country_code"], _today)
                
            # Wait for a while
            if _waiting_for > 0:
                time.sleep(_waiting_for)
                
        # Batch is done. We wait for a while
        if _global_waiting_for > 0:
            time.sleep(_global_waiting_for)
        
            
    # That's all, folks ! Release the lock then exit.
    script.script_exit(p_err=_err)
    
    return
    
if __name__ == "__main__":
    main()