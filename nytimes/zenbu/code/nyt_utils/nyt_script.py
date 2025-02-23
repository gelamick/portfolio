# ============================================================================
#
# File    : nytscript.py
# Date    :
# (c)     : Michaël Abergel - Alfred Quitman - Emmanuel Bompard
# Object  : Defines a class for NYT project scripts and a class with some
#           useful constants
#
# ============================================================================

## Imports
#
import os
import sys
import glob
import time
import pprint
import logging

from enum import Enum

from pymongo import MongoClient
import yaml
import json

##
# Constants class
class NYTErr(Enum):
    # Error codes
    ERR_OK = 0
    ERR_NO_SUCH_PATH = 10
    ERR_PATH_ALREADY_EXISTS = 20
    ERR_NO_CONFIG = 30
    ERR_NO_DATA = 200
    ERR_UNWIND_ISSUE = 220
    ERR_NO_DB_CONNECTION = 300
    ERR_DB_INSERTION = 300
    
    def __lt__(self, other):
        if isinstance(other, NYTErr):
            return self.value < other.value
        return NotImplemented


##
# Script class

class NYTScript():
    # This class provides one sole "instance" (it allows us to keep the same
    # script context anywhere in the same running script (e.g. with multiple
    # sub-scripts launched by Dash))
    _instance = None
    _initialized = False
    
    # New instance : only one instance in the same application
    # (useful singleton pattern !)
    def __new__(cls, p_filepath):
        if cls._instance is None:
            cls._instance = super(NYTScript, cls).__new__(cls)
        return cls._instance        
    
    # Main constructor (doesn't do much)
    def __init__(self, p_filepath):
        """
        Simply initialise attributes to None
        """
        if not self._initialized:
            # Get base/bare names
            _ = self.get_script_name()
    
            # Configuration
            _ = self.get_config(p_filepath)
            
            # Logging initialisation
            _ = self.init_logging()
            
            # Database connection
            self.client = self.get_connection()
    
            # Get/create the database
            self.db = self.client[self.d_config["database"]["db_name"]]
        
        # Initialized
        self._initialized = True

        return

    def get_script_name(self):
        """
        Returns the bare script name and sets the matching attributes
        """
        self.basename = os.path.basename(sys.argv[0])
        self.barename, _ = os.path.splitext(self.basename)
        return self.barename
    
    def check_create_dir(self, p_dir_path):
        """
        Simply checks whether the path exists and creates it if it does not
        """
        if not os.path.exists(p_dir_path):
            # $$$$ TODO : use logging but there is a problem of handler precedence.
            print(f"Creation of {p_dir_path}")
            os.makedirs(p_dir_path)
        return p_dir_path
    
    def rename_file(self, p_from, p_to, p_force=True):
        """
        Rename (or move, as UN*X does) a file.
        If the destination already exists, the move can be forced. If not, nothing
        happen and an error is returned.
        
        Returns an error code (slightly old-fashioned...)
        """
        if not os.path.exists(p_from):
            return NYTErr.ERR_NO_SUCH_PATH
        
        if os.path.exists(p_to):
            if p_force:
                os.remove(p_to)
            else:
                return NYTErr.ERR_PATH_ALREADY_EXISTS
        
        os.rename(p_from, p_to)
        return NYTErr.ERR_OK
    
    def get_config(self, p_filepath):
        """
        Loads the YAML configuration file and returns the dictionary.
        """
        with open(p_filepath, "r") as yaml_conf:
            self.d_config = yaml.safe_load(yaml_conf)

        # By default, no lock file has been set up
        self.d_config["lock_file_path"] = None

        return self.d_config
    
    def init_logging(self):
        """
        Log handler initialisation
        
        Returns OK if OK :-), an error code otherwise. 
        """
        try:
            # Create the logging directory
            _path = self.check_create_dir(os.path.join(self.d_config["data_dir"],
                                                       self.d_config["logs_sub_dir"]))
            
            # Initialisation
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(os.path.join(_path, f"{self.barename}.log")),
                    logging.StreamHandler()
                ]
            )
            return NYTErr.ERR_OK
        except Exception as e:
            logging.error("Problem occurred while locking script. Probably missing configuration... (e)")
            return NYTErr.ERR_NO_CONFIG
        
    def get_lock_path(self):
        """
        Returns the path of the script's lock file
        """
        try:
            _path = self.check_create_dir(os.path.join(self.d_config["data_dir"],
                                                       self.d_config["lock_sub_dir"]))
            return os.path.join(_path, f"{self.barename}.lock")
        except Exception as e:
            logging.error("Problem occurred while getting lock path. Probably missing configuration... (e)")
            raise
    
    def test_and_lock(self, p_force=True):
        """
        Checks whether there's already a running script : creates the file if
        it doesn't already exists, exit otherwise.
        """
        try:
            self.d_config["lock_file_path"] = self.get_lock_path()
        
            if not os.path.exists(self.d_config["lock_file_path"]):
                with open(self.d_config["lock_file_path"], "w") as file:
                    file.write("Work in progress...")
            else:
                logging.error(f"Another instance of the script is already running.")
                if not p_force:
                    sys.exit(1)
        except Exception as e:
            logging.error("Problem occurred while test/lock-ing script. Probably missing configuration... (e)")
            raise
    
    def remove_lock(self):
        """
        Removes the script's lock file.
        """
        try:
            if self.d_config["lock_file_path"] is not None:
                os.remove(self.d_config["lock_file_path"])
        except Exception as e:
            logging.error("Problem occurred while removing lock. Probably missing configuration... (e)")
            return
    
    def script_exit(self, p_err=NYTErr.ERR_OK):
        """
        Removes the script's lock file and exits.
        """
        try:
            self.remove_lock()
            sys.exit(p_err)
        except Exception as e:
            logging.error("Problem occurred while exiting script. (e)")
            raise
        
    
    # ----------------------------------------------------------------------------
    # 
    # Database related functions ($$$ TODO : maybe use a separated class instead)
    #
    
    def get_connection(self):
        """
        Get mongoDB connection
        """
        _s_user = f'{self.d_config["database"]["db_user"]}:{self.d_config["database"]["db_pass"]}'
        _s_server = f'{self.d_config["database"]["db_host"]}:{self.d_config["database"]["db_port"]}'
        _s_cx = f'mongodb://{_s_user}@{_s_server}'
        try:
            _client = MongoClient(_s_cx)
        except Exception as e:
            logging.error(f"Could not connect to database server: {e}")
            self.script_exit(p_err = NYTErr.ERR_NO_DB_CONNECTION)
        
        logging.info(f"Connected to mongoDB server ({_s_cx})")
        self.d_config["database"]["connection"] = _client
        return _client
