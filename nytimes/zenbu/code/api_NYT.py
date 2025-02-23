# ============================================================================
#
# File    : api_NYT.py
# Date    : 2024/09/27
# (c)     : Michaël Abergel - Alfred Quitman - Emmanuel Bompard
# Object  : NYT project exposed API.
# Version : 0.3.0
#
# ============================================================================

##
# Basic imports (from the library)
import os
import random

##
# FastAPI and friends
from fastapi import Depends, FastAPI, HTTPException, status, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import JSONResponse
import bcrypt

##
# To help structure the data sent/received
from pydantic import BaseModel
from typing import Optional, List

##
# Data handling
import pandas as pd
import json
import joblib

##
# Project related modules
from code.nyt_utils.nyt_script import NYTErr, NYTScript
from code.nyt_utils.nyt_dbqueries import NYTDBQueries
from code.nyt_utils.nyt_webscrap import NYTWebScrap

#----------------------------------------------------------------------

##
# Functions to deal w/ passwords, using bcrypt

def hash_string(p_str: str) -> str:
    """
    Hash the given string and return the result.
    """
    # Convert the string into a UTF-8 string
    _s_utf8 = p_str.encode('utf-8')

    # Hash the string
    _s_hashed = bcrypt.hashpw(password=_s_utf8, salt=bcrypt.gensalt())
    
    return _s_hashed

def check_pwd(p_str: str, p_pwd: str) -> bool:
    """
    Check a string against a hashed password and returns True if the hashes match.
    """
    # Convert the string into a UTF-8 string
    _s_utf8 = p_str.encode('utf-8')

    return bcrypt.checkpw(password=_s_utf8 , hashed_password=p_pwd)

#----------------------------------------------------------------------

##
# Initialisations

# API instanciation
g_api_label = "NYT API API"
g_api = FastAPI(
    title = g_api_label,
    description = "NYT project exposed API",
    version = "0.1.0"
)

# Authentication scheme (basic) :
security = HTTPBasic()

# Script instanciation : get MongoDB connection and other
# project related context
script = NYTScript(os.environ["NYT_CONFIG_FILE"])

# Deserialize the ML model and tools
# SVM Model
script.svm_model = joblib.load(os.path.join(
    script.d_config["ml"]["serial_path"], "svm_model.joblib"
))
# TF-IDF vectorizer
script.tfidf = joblib.load(os.path.join(
    script.d_config["ml"]["serial_path"], "tfidf_vectorizer.joblib"
))


#----------------------------------------------------------------------

# User database

# Just for fun, there are some roles :
#  - "simple" can call normal endpoints
#  - "admin" can only access administrative endpoints
#  - "power" can call DB modifying endpoints and normal endpoints
#
# At this point, there aren't any admin or power things to do but one
# could consider "power" as role for some internal callers while
# external callers would be "simple", for instance.

g_d_users = {
    "admin": {
        "user_id": 0,
        "name": "admin",
        "pwd": hash_string("0-ATOMIC-brisling-fondue-paleface-$"),
        "role": ["admin"],
    },
    "alice": {
        "user_id": 1001,
        "name": "alice",
        "pwd": hash_string("wonderland"),
        "role": ["simple"],
    },
    "bob": {
        "user_id": 1002,
        "name": "bob",
        "pwd": hash_string("builder"),
        "role": ["simple", "power"]
    },
    "gustave": {
        "user_id": 1003,
        "name": "gustave",
        "pwd": hash_string("faubourg-de-Carthage-%0$"),
        "role": ["power"]
    },
}

#----------------------------------------------------------------------

##
# Data records (nearly one for each endpoint)

# Years limits request
class YearLimitsRec(BaseModel):
    # This record contains :
    #   - the name of the collection to scan
    #   - the name of the date variable to check
    #   - the expected format of this variable
    collection: str
    date_var: str
    date_fmt: str

# Count by month request
class CountByMonthRec(BaseModel):
    # This record contains :
    #   - the name of the collection to scan
    #   - the list of variables to consider
    #   - the name of the date variable
    #   - the format of this variable
    collection: str
    vars: List[str]
    date_from: str
    date_to: str
    date_var: str
    date_fmt: str
    ascending: bool

# Simple record for collection parameter
class CollectionRec(BaseModel):
    # One sole item : the collection name
    collection: str

# Record for keyword search
class KWRec(BaseModel):
    # One sole item : the collection name
    collection: str
    date_from: str
    date_to: str

# List of books request
class ListBooksRec(BaseModel):
    # This record contains :
    #   - the name of the collection to scan
    #   - the starting date
    #   - the ending date
    #   - the list of lists' IDs to search into
    collection: str
    from_date: str
    to_date: str
    id_list: List[int]

# Book price request
class BookPriceRec(BaseModel):
    # This record contains :
    #   - the ISBN-10 code of the book to price
    #   - the country code determining which Amazon website to query
    isbn10: str
    country: str

# Prediction request
class PredictionRec(BaseModel):
    # This record contains :
    #   - the collection to query
    #   - the article's NYT ID 
    collection: str
    nyt_id: str

#----------------------------------------------------------------------

##
# User checking functions

def get_request_user(p_creds: HTTPBasicCredentials = Depends(security)):
    """
    Checks the credentials against the user database
    """
    _user = g_d_users.get(p_creds.username)
    if not _user or not(check_pwd(p_creds.password, _user["pwd"])):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect ID or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return _user

#----------------------------------------------------------------------

##
# ML related functions

def make_article_prediction(p_data:list[dict]) -> dict:
    """
    Get the prediction for the given data : namely a list of dictionaries
    matching an article ID. In fact, one expects only ONE item but several
    would be fine as well.
    """
    # Create a small dataframe from the data
    _df = pd.DataFrame(p_data)
        
    # Aggregate the two interesting fields :
    _df["combined_text"] = _df["headline"] + " " + _df["lead_paragraph"]
    
    # Transform data
    X_new_tfidf = script.tfidf.transform(_df["combined_text"])
    
    # Transform, predict and add the result to the dataframe
    _df["predicted_section"] = script.svm_model.predict(script.tfidf.transform(_df["combined_text"])) 

    # Return the dataframe
    return _df

#----------------------------------------------------------------------
#
# API Endpoints definitions

##
# Generics :

@g_api.get("/verify")
def check_api(p_user: str = Depends(get_request_user)):
    """
    This endpoint simply checks the credentials of the requester and
    say hello (always polite). It is also a means to know whether the
    API is up and running.
    
    Valid credentials (user/password) have to be sent in headers.
      
    Returns :
      - some welcome message
    """
    return {
        "message": f"Hi {p_user['name']} ! {g_api_label} is available for requests."
    }

@g_api.post("/year_limits")
def year_limits(
    p_request: YearLimitsRec,
    p_user: str = Depends(get_request_user)):
    """
    This endpoint returns the date limits for the given collection
    of the database, meaning : the date of the oldest (resp. most recent)
    document.
    
    The parameters are :
      - collection : the collection to scan
      - date_var : the date field of the collection's documents
      - date_fmt : the expected date format of the date field
    
    Valid credentials (user/password) have to be sent in headers.

    Returns :
      - year_min and year_max of the collection
    """
    # First of all, check whether the user has the right role
    if "simple" not in p_user["role"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"User {p_user['name']} is not allowed to query this endpoint.",
        )
    
    # Request the database and get the expected limits
    _year_min, _year_max = NYTDBQueries.year_limits(
        script.db[p_request.collection],
        p_date_field = p_request.date_var,
        p_format = p_request.date_fmt
    )

    return {
        "collection": p_request.collection,
        "year_min": _year_min,
        "year_max": _year_max,
    }

@g_api.post("/articles/count_by_month")
def count_by_month(
    p_request: CountByMonthRec,
    p_user: str = Depends(get_request_user)):
    """
    This endpoint counts the values of the given categorical vars and their
    respective number of occurrences, relatively of months.

    The parameters are :
      - collection : the collection to scan
      - vars : a list of fields (used in collection's documents)
      - date_from : the starting date as YYYY-MM-DD ("%Y-%m-%d")
      - date_to : the ending date as YYYY-MM-DD ("%Y-%m-%d")
      - date_var : the date field of the collection's documents
      - date_fmt : the expected date format of the date field
      - ascending : results sorting order
    
    Valid credentials (user/password) have to be sent in headers.

    The collection name has to be provided just in case several collections
    of the same type would be maintained.
    
    Returns :
      - a serialized dataframe
    """
    # First of all, check whether the user has the right role
    if "simple" not in p_user["role"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"User {p_user['name']} is not allowed to query this endpoint.",
        )
    
    # Request the database and get the expected dataframe
    _df = NYTDBQueries.count_by_month(
        script.db[p_request.collection],
        p_request.vars,
        p_request.date_from,
        p_request.date_to,
        p_date_field = p_request.date_var,
        p_ascending = p_request.ascending,
        p_format = p_request.date_fmt,
    )

    return {
        "collection": p_request.collection,
        "result": _df.to_dict(orient="records"),
    }

@g_api.post("/articles/count_keywords")
def count_keywords(
    p_request: KWRec,
    p_user: str = Depends(get_request_user)):
    """
    This endpoint counts the keywords, their rank, their number of
    occurrences.
    The parameter is :
      - collection : the collection to scan
      - date_from : the starting date as YYYY-MM-DD ("%Y-%m-%d")
      - date_to : the ending date as YYYY-MM-DD ("%Y-%m-%d")

    Valid credentials (user/password) have to be sent in headers.

    The collection name has to be provided just in case several collections
    of the same type would be maintained.
    
    Note : it is too heavy a request and should be improved with some
      new parameters such as section, dates and so on.
    
    Returns :
      - a serialized dataframe
    """
    # First of all, check whether the user has the right role
    if "simple" not in p_user["role"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"User {p_user['name']} is not allowed to query this endpoint.",
        )
    
    # Request the database and get the expected dataframe
    _df = NYTDBQueries.count_arch_keywords(
        script.db[p_request.collection],
        p_request.date_from, p_request.date_to,
    )

    return {
        "collection": p_request.collection,
        "result": _df.to_dict(orient="records"),
    }

@g_api.post("/articles/predict")
def predict(
    p_request: PredictionRec,
    p_user: str = Depends(get_request_user)):
    """
    Endpoint to predict section names for an article.
    
    Returns the fields use to predict the section name, the actual section
    name and the predicted one/

    Parameters:
      - nyt_id: the article's NYT ID 
    """
    # First of all, check whether the user has the right role
    if "simple" not in p_user["role"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"User {p_user['name']} is not allowed to query this endpoint.",
        )
    
    # Request the database and get the expected document
    _l_articles = NYTDBQueries.archive_get(script.db[p_request.collection], p_request.nyt_id)
    
    # If no articles were found raises an error
    if len(_l_articles) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No article matching '{p_request.nyt_id}' where found",
        )
    
    # Make the prediction
    _l_data = [
        {"headline": x["headline"]["main"], "lead_paragraph": x["lead_paragraph"], "section_name": x["section_name"]}
        for x in _l_articles
    ]
    _df = make_article_prediction(_l_data)
    
    # Here we're only interested in the first (and sole, actually) item
    _d_article = _df.iloc[0].to_dict()

    return {
        "nyt_id": p_request.nyt_id,
        "headline": _d_article["headline"],
        "lead_paragraph": _d_article["lead_paragraph"],
        "actual_section": _d_article["section_name"],
        "predicted_section": _d_article["predicted_section"],
    }

@g_api.post("/books/lists")
def lists_lists(
    p_request: CollectionRec,
    p_user: str = Depends(get_request_user)):
    """
    This endpoint returns the lists' lists of the Books collection.
    The parameter is :
      - collection : the collection to scan

    Valid credentials (user/password) have to be sent in headers.

    The collection name has to be provided just in case several collections
    of the same type would be maintained.
    
    Returns :
      - a serialized dataframe (it is a small one)
    """
    # First of all, check whether the user has the right role
    if "simple" not in p_user["role"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"User {p_user['name']} is not allowed to query this endpoint.",
        )
    
    # Request the database and get the expected dataframe
    _df = NYTDBQueries.list_lists(script.db[p_request.collection])

    return {
        "collection": p_request.collection,
        "result": _df.to_dict(orient="records"),
    }

@g_api.post("/books/list_books")
def list_books(
    p_request: ListBooksRec,
    p_user: str = Depends(get_request_user)):
    """
    This endpoint returns a list of books obtained by filtering the Books
    collection according to the parameters :
      - collection : the collection to scan
      - from_date : the starting date [YYYY-MM-DD]
      - to_date : the ending date [YYYY-MM-DD]
      - id_list : the lists to search into (specified by the list of their IDs)
      
    Valid credentials (user/password) have to be sent in headers.

    The collection name has to be provided just in case several collections
    of the same type would be maintained.
    
    Returns :
      - a serialized dataframe (it is a small one)
    """
    # First of all, check whether the user has the right role
    if "simple" not in p_user["role"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"User {p_user['name']} is not allowed to query this endpoint.",
        )
    
    # Request the database and get the expected dataframe
    _df = NYTDBQueries.list_books(
        script.db[p_request.collection],
        p_request.from_date,
        p_request.to_date,
        p_request.id_list,
    )

    return {
        "collection": p_request.collection,
        "result": _df.to_dict(orient="records"),
    }

@g_api.post("/books/price")
def list_books(
    p_request: BookPriceRec,
    p_user: str = Depends(get_request_user)):
    """
    This endpoint returns a book price obtained from an Amazon web site.
    The parameters are :
      - isbn10 : the book's ISBN10
      - country : a country code to know which Amazon web site will provide the price
    
    Valid credentials (user/password) have to be sent in headers.

    Returns :
      - a string giving the price ; or "unknown price" if something
        went wrong.
    """
    # First of all, check whether the user has the right role
    if "simple" not in p_user["role"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"User {p_user['name']} is not allowed to query this endpoint.",
        )
    
    # Request the database and get the expected dataframe
    _item_price = NYTWebScrap.amazon_price(
        p_request.isbn10,
        p_country = p_request.country,
    )

    return {
        "status": "OK" if _item_price != "unknown price" else "KO",
        "result": _item_price,
    }

@g_api.post("/books/random_price")
def list_books(
    p_request: BookPriceRec,
    p_user: str = Depends(get_request_user)):
    """
    This endpoint returns a random book price as it would be obtained from an
    Amazon web site. Useful for testing purpose when you cannot connect to anything
    (as in a plane, back from vacations...)

    The parameters are :
      - isbn10 : the book's ISBN10
      - country : a country code to know which Amazon web site will provide the price
    
    Valid credentials (user/password) have to be sent in headers.

    Returns :
      - a string giving the price ; or "unknown price" if something
        went wrong.
    """
    # First of all, check whether the user has the right role
    if "simple" not in p_user["role"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"User {p_user['name']} is not allowed to query this endpoint.",
        )
    
    # Gets two numbers
    _n_curr = random.randint(0,30)
    _n_cent = random.randint(0,99)

    # Choose the currency depending on the country
    match p_request.country:
        case "FR" | "DE" | "IT" :
            _s_curr = "€"
            _item_price = f"{_n_curr},{_n_cent:02d} {_s_curr}"
        case "JP":
            _s_curr = "¥"
            _item_price = f"{_s_curr}{_n_curr}.{_n_cent:02d}"
        case "UK":
            _s_curr = "£"
            _item_price = f"{_s_curr}{_n_curr}.{_n_cent:02d}"
        case "US":
            _s_curr = "$"
            _item_price = f"{_s_curr}{_n_curr}.{_n_cent:02d}"
        case _:
            _s_curr = None

    if _s_curr is None:
        _item_price = "unknown price"

    return {
        "status": "OK" if _item_price != "unknown price" else "KO",
        "result": _item_price,
    }

