# ============================================================================
#
# File    : load_NYT_db.py
# Date    : 2024/09/09
# (c)     : Michaël Abergel - Alfred Quitman - Emmanuel Bompard
# Object  : Loads JSON files fetched from NYT APIs into a mongoDB database.
#           Several collections may be loaded.
#           At this stage :
#             - data are inserted "as is" but later on filters
#               shall be applied.
# Version : 0.3.0
#
# ============================================================================

## Imports
#
import os
import time
import glob
import logging
from functools import reduce

from pymongo import MongoClient
import yaml
import json

##
# Project imports
from nyt_utils.nyt_script import NYTErr, NYTScript

# ============================================================================

def get_payload(p_dict, p_keys):
    """
    Gets the payload nested in the dictionary and denoted by the
    list of keys to access it.
    
    Parameters :
      - p_dict : the dictionary to browse
      - p_keys : the key list to access the data
      
    Return :
      - return what has been found
      - or None if nothing has been found
    """
    try:
        _res = reduce(lambda x, y: x[y], p_keys, p_dict)
    except KeyError:
        logging.error(f" Payload error : a key does not exist ! ")
        _res = None
    except TypeError:
        logging.error(f" Payload error : a key points to non-dictionary value! ")
        _res = None
    return _res

def unwind_dict(p_dict, p_key, *, p_list_dup=None):
    """
    This function "unwind" the dictionary in a similar way MongoDB does in an
    aggregation pipeline. For now, we keep things simple and consider that the
    key is at the dictionary top level and that all other keys are to be
    duplicated, except if a list is given.
    
    Parameters :
      - p_dict : the dictionary to unwind
      - p_key : the list the unwind is based on
      - p_list_dup : the keys to duplicate while unwinding ; default : None

    Return a list of dictionaries.
    """
    # Get the list of keys to duplicate
    if p_list_dup is None:
        # If not given take all keys save the one to unwind
        try:
            _l_dup = list(p_dict.keys())
            _l_dup.pop(_l_dup.index(p_key))
        except Exception as e:
            return None
    else:
        _l_dup = p_list_dup

    # Instantiate the new list
    _l = list()
    
    # Create a dictionary of all duplicates :
    _d_dup = { k: p_dict[k] for k in _l_dup }
    
    # Loop thru the list and add entries :
    for _v in p_dict[p_key]:
        # Append a merge of duplicates dict and current item
        _l.append(_d_dup | _v)
            
    return _l

# ----------------------------------------------------------------------------
# 
# Database related functions
#

def open_load_file(p_script, p_coll_def, p_path):
    """
    Opens the file, gets its data and import it in the right database collection.
    """
    _err = NYTErr.ERR_OK
    
    _basename = os.path.basename(p_path)
    
    # Load json into a variable ...
    logging.info(f" >> reads {_basename}")
    with open(p_path, "r") as _file:
        _file_data = json.load(_file)
        
    # ... and get the payload. At this point it may still be a dictionary.
    if (_docs := get_payload(_file_data, p_coll_def["payload_path"])) is None:
        return NYTErr.ERR_NO_DATA
    
    # Check whether we have to unwind some iterable (MongoDB isn't able to do so)
    if "unwind_key" in p_coll_def:
        # Let's unwind and get a list
        if (_docs := unwind_dict(_docs, p_coll_def["unwind_key"])) is None:
            return NYTErr.ERR_UNWIND_ISSUE
    
    # We may have only one item : check whether there's a list
    if not isinstance(_docs, list):
        _docs = [_docs]
    
    # Take only the requested fields and rename them
    _new_docs = []
    logging.info(f" >> filters {_basename} ")
    for _doc in _docs:
        _new_doc = dict()
        for _kf, _of in zip(p_coll_def["kept_field"], p_coll_def["output_field"]):
            if (_v := _doc.get(_kf)) is not None:
                _new_doc[_of] = _v
        _new_docs.append(_new_doc)
    
    # Load the data into the database collection
    logging.info(f" >> loads {_basename} in collection {p_coll_def['coll_name']}")
    _db_collection = p_script.db[p_coll_def["coll_name"]]
    try:
        _result = _db_collection.insert_many(_new_docs)
        logging.info(f" >> inserted {len(_result.inserted_ids)} articles into collection {p_coll_def['coll_name']}.")
    except Exception as e:
        logging.info(f" >> error during database insertion : {e}.")
        _err = NYTErr.ERR_DB_INSERTION
    
    return _err

def open_and_load_coll(p_script, p_coll_def):
    """
    Loads the data for the specified collection configuration, into the ad hoc
    database/collection.
    
    Basically take all JSON files available and load them sequentially.
    In the process, files are moved from one folder to another :
      - if OK : input => processing => processed
      - if KO : input => processing => failed
    If something goes wrong the files to check will probably be those in the "failed"
    state.  
    """
    _err = NYTErr.ERR_OK
    
    # First things first : check/create the needed directories
    _path_input = p_script.check_create_dir(os.path.join(p_script.d_config["data_dir"],
                                                         p_coll_def["input_sub_dir"]))
    _path_processing = p_script.check_create_dir(os.path.join(p_script.d_config["data_dir"],
                                                           p_coll_def["processing_sub_dir"]))
    _path_failed = p_script.check_create_dir(os.path.join(p_script.d_config["data_dir"],
                                                          p_coll_def["failed_sub_dir"]))
    _path_processed = p_script.check_create_dir(os.path.join(p_script.d_config["data_dir"],
                                                             p_coll_def["processed_sub_dir"]))
        
    # Now scan the input folder and load each file
    _l_files = glob.glob(os.path.join(_path_input, f'*.{p_coll_def["input_ext"]}'))
    
    # Errors set
    _err_set = set()

    for _file_name in _l_files:
        # Names / states
        _file_basename = os.path.basename(_file_name)
        _file_processing_name = os.path.join(_path_processing, _file_basename)
        _file_failed_name = os.path.join(_path_failed, _file_basename)
        _file_processed_name = os.path.join(_path_processed, _file_basename)
        
        #
        logging.info(f"Processing : {_file_basename}")
        p_script.rename_file(_file_name, _file_processing_name)
        if (_err_load := open_load_file(p_script, p_coll_def, _file_processing_name)) == NYTErr.ERR_OK:
            logging.info(f"Processed : {_file_basename}")
            p_script.rename_file(_file_processing_name, _file_processed_name)
        else:
            logging.info(f"Failed : {_file_basename}")
            p_script.rename_file(_file_processing_name, _file_failed_name)
            _err_set.add(_err_load)
        
        _err = max(_err, _err_load)
        
    return _err, _err_set
    

# ============================================================================

def main():
    """
    """
    # Instanciate a new script
    script = NYTScript(os.environ["NYT_CONFIG_FILE"])
    
    # Then, let's check whether a similar script is already running
    script.test_and_lock()
    
    # Errors set (for now not used as such but next sprint it will be...)
    _err_set = set()
    _err = NYTErr.ERR_OK

    # Runs through the configuration's collections dict and loads the matching data
    _keep_going = True
    _waiting_for = script.d_config["db_load"]["waiting_for"]
    while _keep_going:
        for _coll_def in script.d_config["collections"].values():
            logging.info(f"Loading data for '{_coll_def['coll_name']}'")
            _err_load, _err_subset = open_and_load_coll(script, _coll_def)
            
            _err = max(_err, _err_load)
            _err_set = set.union(_err_set, _err_subset)

        if _waiting_for <= 0:
            _keep_going = False
        else:
            time.sleep(_waiting_for)
    
    # That's all, folks ! Release the lock then exit.
    script.script_exit(p_err=_err)
    
    return
    
if __name__ == "__main__":
    main()