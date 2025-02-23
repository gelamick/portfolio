# ============================================================================
#
# File    : nyt_dbqueries.py
# Date    : 2024/09/07
# (c)     : Michaël Abergel - Alfred Quitman - Emmanuel Bompard
# Object  : Defines a class of static methods (stateless methods, actually)
#           carrying out queries against a MongoDB / DB / collection.
#           May be converted in a normal class if needed.
# Version : 0.1.0
#
# ============================================================================

## Imports
#
import logging
from pymongo import MongoClient
from pymongo.collection import Collection

import pandas as pd
import numpy as np

import datetime

class NYTDBQueries():
    
    ###
    # Mostly generic queries
    
    @staticmethod
    def year_month(p_row: pd.Series, *, p_sep: str = "/") -> str:
        """
        Concatenate the row's "year" and "month" columns values of a pandas
        dataframe into a single string.
        """
        return f"{p_row['year']}{p_sep}{int(p_row['month']):02d}"
    
    @staticmethod
    def year_limits(p_coll, *,
                    p_date_field: str = "pub_date",
                    p_format: str = "%Y-%m-%dT%H:%M:%S%z") -> (int, int):
        """
        Get the min and max years of the collection documents.
        """
        # Aggregation pipeline
        _ppl = [
            # We need only one field
            {
                "$project": { f"{p_date_field}": f"${p_date_field}" }
            },
            # Create a real date from the string
            {
                "$set": {
                    "real_date": {
                        "$dateFromString": {
                            "dateString": f"${p_date_field}",
                            "format": f"{p_format}"
                        }
                    }
                }
            },
            # Extract the year
            {
                "$set": {
                    "year": { "$year": "$real_date" }
                }
            },
            # Get min and max
            {
                "$group": {
                    "_id": 0,
                    "year_min": { "$min": "$year" },
                    "year_max": { "$max": "$year" },
                }
            },
        ]
        
        logging.info(f"New pipeline : {_ppl}")
    
        # Run the aggregation and convert the results into a list
        # (there's only one line, actually)
        _res = list(p_coll.aggregate(_ppl))
    
        return _res[0]["year_min"], _res[0]["year_max"]
    
    @staticmethod
    def value_counts(p_coll:Collection, p_var, *, p_ascending=False):
        """
        Count the occurrences and values of the categorical var.
        """
        # Aggregation pipeline
        _asc = 1 if p_ascending else -1
        _ppl = [
            { "$group": { "_id": f"${p_var}", "count": { "$sum": 1 } } },
            { "$sort": { "count": _asc } }
        ]
    
        logging.info(f"New pipeline : {_ppl}")
    
        # Run the aggregation
        _res = p_coll.aggregate(_ppl)
    
        # Convert results to a DataFrame
        _df = pd.DataFrame(list(_res))
    
        return _df
    
    @staticmethod
    def count_by_month(p_coll:Collection, p_vars, p_from_date, p_to_date, *,
                       p_date_field="pub_date", 
                       p_format="%Y-%m-%dT%H:%M:%S%z"):
        """
        Count the occurrences and values of the categorical var, by month.

        Parameters :
          - p_coll : the collection
          - p_vars : the variables to filter
          - p_from_date : the starting date
          - p_to_date : the ending date
          - p_date_field : the name of the date field to use
          - p_format : the format of the date field (the date field is
              a string denoting a date and this is its format)
    
        Return : the pandas dataframe
        """
        # Variables to take into account : one or a list
        l_vars = [p_vars] if isinstance(p_vars, str) else p_vars
        # Hence the group definition :
        _d_id = {
            "month": { "$month": "$real_date" },
            "year": { "$year": "$real_date" },
        }
        _d_id.update({ v: f"${v}" for v in l_vars})

        # We filter entries by date : so we build date string in the right
        # format.
        if (p_format != "%Y-%m-%d"):
            _from_date = datetime.datetime.strptime(p_from_date, "%Y-%m-%d").strftime(p_format)
            _to_date = datetime.datetime.strptime(p_to_date, "%Y-%m-%d").strftime(p_format)
        else:
            _from_date = p_from_date
            _to_date = p_to_date
            
        # Aggregation pipeline
        _ppl = [
            # Filter on some dates
            {
                "$match": {
                    f"{p_date_field}": {
                        "$gte": f"{_from_date}",
                        "$lte": f"{_to_date}",
                    },
                }
            },
            # Convert the date (string) into a real date field
            {
                "$addFields": { "real_date": {
                        "$dateFromString": { "dateString": f"${p_date_field}",
                                             "format": f"{p_format}" }
                    }
                }
            },
            # Groupby : for each month/year count the number of items in the
            # categories
            {
                "$group": { "_id": _d_id, "count": { "$sum" : 1 } }
            },
        ]
    
        logging.info(f"New pipeline : {_ppl}")
    
        # Run the aggregation ...
        _res = p_coll.aggregate(_ppl)
    
        # ... then convert results into a dataFrame ...
        _df = pd.json_normalize(list(_res))
    
        # ... then change the columns name :
        _d_rename = { "_id.year": "year", "_id.month": "month", "count": "count" }
        _d_rename.update({ f"_id.{v}": v for v in l_vars})
    
        _df = _df.rename(columns=_d_rename)[l_vars + ["year", "month", "count"]]
    
        return _df

    ###
    # Archives queries
    @staticmethod
    def archive_get(p_coll:Collection, p_nyt_id: str) -> list[dict] :
        """
        Get the list of documents with the given index. There should be only
        one document but who knows...
        
        The caller will take whatever document suits it best.
        
        Parameters :
          - p_coll : the collection
          - p_nyt_id : the document NYT index (NOT the MongoDB index)
        
        Return :
          - a list of entries (normally it should be a singleton)
        """
        # Gets the article : either the ID is full or we complete it
        _id_uri = "nyt://article/"
        _nyt_id = f"{_id_uri}{p_nyt_id}" if not p_nyt_id.startswith(_id_uri) else p_nyt_id
        
        return list(p_coll.find({"nyt_id": f"{_nyt_id}"}))
    
    @staticmethod
    def count_arch_keywords(p_coll:Collection, p_from_date, p_to_date, *,
                            p_date_field="pub_date",
                            p_format="%Y-%m-%dT%H:%M:%S%z"):
        """
        Yields a Pandas dataframe with information related to the "keywords"
        array of collection's documents.
    
        The dataframe contains the following columns :
          - "keyword" : the keyword itself
          - "kind" : the keyword kind : person, subject etc.
          - "ranking" : the keyword rank in its document
          - "year" : the document year
          - "month" : the document month
          - "count" : the number of occurrences of the grouped previous fields
            in the collection.
    
        Parameters :
          - p_coll : the collection
          - p_from_date : the starting date
          - p_to_date : the ending date
    
        Return : the pandas dataframe
    
        Note :
          - a keyword has only one kind
        """
        # We filter entries by date : so we build date string in the right
        # format.
        if (p_format != "%Y-%m-%d"):
            _from_date = datetime.datetime.strptime(p_from_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%S%z")
            _to_date = datetime.datetime.strptime(p_to_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%S%z")
        else:
            _from_date = p_from_date
            _to_date = p_to_date
            
        # Aggregation pipeline
        _ppl = [
            # Filter on some dates
            {
                "$match": {
                    f"{p_date_field}": {
                        "$gte": f"{_from_date}",
                        "$lte": f"{_to_date}",
                    },
                }
            },
            # Get the date as a real date field rather than a string
            {
                "$set": {
                    "real_date": {
                        "$dateFromString": {
                            "dateString": "$pub_date",
                            "format": "%Y-%m-%dT%H:%M:%S%z",
                        }
                    }
                }
            },
            # Unwind the "keywords" array
            {
                "$unwind": "$keywords"
            },
            # Fields projection
            {
                "$project": {
                    # Exclude the document ID
                    "_id": 0,
                    "name": "$keywords.name",
                    "value": "$keywords.value",
                    "rank": "$keywords.rank",
                    "year": { "$year": "$real_date" },
                    "month": { "$month": "$real_date" },
                }
            },
            # The rank can be really high and thus alter the weighted
            # arithmetic mean or tamper the distribution. So we limit
            # its value to an arbitrary value (it should be defined by
            # the variable description but right now, we keep things
            # simple...)
            {
                "$set": {
                    "rank": {
                        "$cond": {
                        "if": { "$gt": ["$rank", 20] },
                        "then": 20,
                        "else": "$rank"
                        }
                    }
                }
            },
            # Group by keyword value, name, rank and date and count the
            # occurrences
            {
                "$group": {
                    "_id": {
                        "name": "$name",
                        "value": "$value",
                        "rank": "$rank",
                        "year": "$year",
                        "month": "$month"
                    },
                    "count": {
                        "$sum": 1
                    }
                }
            },
            # Sort by decreasing counts
            {
                "$sort": {
                    "count": -1
                }
            }
        ]
    
        logging.info(f"New pipeline : {_ppl}")
    
        # Run the aggregation ...
        _res = p_coll.aggregate(_ppl)
    
        # ... then convert results into a dataFrame ...
        _df = pd.json_normalize(list(_res))
    
        # ... then change the columns name :
        _d_rename = {
            "_id.value": "keyword",
            "_id.name": "kind",
            "_id.rank": "rank",
            "_id.year": "year",
            "_id.month": "month",
            "count": "count",
        }
        _l_cols = ["keyword", "kind", "rank", "year", "month", "count"]
    
        # Rename and reorder the columns then return the dataframe
        _df = _df.rename(columns=_d_rename)[_l_cols]
    
        return _df

    ###
    # Books / bestsellers queries
    
    @staticmethod
    def list_lists(p_coll:Collection):
        """
        Get the list of all lists in the collection, with their number of
        occurrences.
    
        Parameters :
          - p_coll : the collection
    
        Return : the pandas dataframe
    
        Note :
          - lists are always the same : same id, same name etc. (it's been
            checked against the current 10 years database).
        """
        # The aggregation pipeline :
        _ppl = [
            # Selection of useful fields
            {
                "$project": {
                    "_id": 0,
                    "list_id": "$list_id",
                    "list_name": "$list_name",
                    "list_encoded": "$list_name_encoded",
                    "list_display": "$display_name",
                }
            },
            # Group by list : count the number of occurrences
            {
                "$group": {
                    "_id": {
                        "list_id": "$list_id",
                        "list_name": "$list_name",
                        "list_encoded": "$list_encoded",
                        "list_display": "$list_display",
                    },
                    "count": { "$sum": 1 },
                }
            },
            # Sort by count (descending) and name (ascending)
            {
                "$sort": {
                    "count": -1,
                    "_id.list_name": 1,
                }
            }
        ]

        logging.info(f"New pipeline : {_ppl}")
    
        # Run the aggregation ...
        _res = p_coll.aggregate(_ppl)
    
        # ... then convert results into a dataFrame ...
        _df = pd.json_normalize(list(_res))
    
        # ... then change the columns name :
        _df.columns = [x.replace("_id.", "") for x in _df.columns]
    
        return _df

    @staticmethod
    def list_all_books(p_coll:Collection):
        """
        Get the list of all books in the collection, with their number of
        occurrences. It may be slightly heavy so not often used.
    
        Parameters :
          - p_coll : the collection
    
        Return : the pandas dataframe
        """
        # Aggregation pipeline
        _ppl = [
            # Get the date in real format
            {
                "$set": {
                    "publish_real_date": {
                        "$dateFromString": {
                            "dateString": "$published_date",
                            "format": "%Y-%m-%d",
                        }
                    }
                }
            },
            # Now get the year, month, day, week
            {
                "$set": {
                    "publish_year": {"$year": "$publish_real_date"},
                    "publish_month": {"$month": "$publish_real_date"},
                    "publish_day": {"$dayOfMonth": "$publish_real_date"},
                    "publish_week": {"$week": "$publish_real_date"},
                }
            },
            # Unwind the list book array
            {
                "$unwind": {
                    "path": "$books",
                    "preserveNullAndEmptyArrays": False
                }
            },
            # Select the required/useful fields
            {
                "$project": {
                    "_id": 0,
                    "list_id": "$list_id",
                    "list_name": "$list_name",
                    "list_encoded": "$list_name_encoded",
                    "list_display": "$display_name",
                    "title": "$books.title",
                    "author": "$books.author",
                    "publisher": "$books.publisher",
                    "publish_year": "$publish_year",
                    "publish_month": "$publish_month",
                    "publish_day": "$publish_day",
                    "publish_week": "$publish_week",
                    "isbn10": "$books.primary_isbn10",
                    "isbn13": "$books.primary_isbn13",
                    "description": "$books.description",
                    "rank": "$books.rank",
                    "rank_last_week": "$books.rank_last_week",
                    "rank_nb_weeks": "$books.weeks_on_list",
                    "image": "$books.book_image",
                    "image_w": "$books.book_image_width",
                    "image_h": "$books.book_image_height",
                }
            },
            {
                "$group": {
                    "_id": {
                        "author": "$author",
                        "title": "$title",
                        "publisher": "$publisher",
                        "isbn10": "$isbn10",
                        "isbn13": "$isbn13",
                        "rank": "$rank",
                    },
                    "count": {"$sum": 1},
                    "publish_year": {"$first": "$publish_year"},
                    "publish_month": {"$first": "$publish_month"},
                    "publish_day": {"$first": "$publish_day"},
                    "publish_week": {"$first": "$publish_week"},
                    "description": {"$first": "$description"},
                    "image": {"$first": "$image"},
                    "image_w": {"$first": "$image_w"},
                    "image_h": {"$first": "$image_h"},
                    "lists": {"$addToSet": "$list_id"},
                    "list_names": {"$addToSet": "$list_name"},
                }
            },
        ]

        logging.info(f"New pipeline : {_ppl}")
    
        # Run the aggregation ...
        _res = p_coll.aggregate(_ppl)
    
        # ... then convert results into a dataFrame ...
        _df = pd.json_normalize(list(_res))
    
        # ... then change the columns name :
        _df.columns = [x.replace("_id.", "") for x in _df.columns]
        
        return _df

    @staticmethod
    def books_all_isbn10(p_coll:Collection):
        """
        Get the list of all ISBN in the collection.
    
        Parameters :
          - p_coll : the collection
    
        Return :
          - the list of books ISBN
        """
        # Aggregation pipeline
        _ppl = [
            {
                "$unwind": {
                    "path": "$books",
                    "preserveNullAndEmptyArrays": False
                }
            },
            {
                "$group": {
                    "_id": {"isbn10": "$books.primary_isbn10" },
                }
            }
        ]

        logging.info(f"New pipeline : {_ppl}")
    
        # Run the aggregation ...
        _res = p_coll.aggregate(_ppl)
    
        # ... then return the requested list
        return [ x["_id"]["isbn10"] for x in _res ]

    @staticmethod
    def list_books(p_coll:Collection, p_from_date, p_to_date, p_l_list_id):
        """
        Get the list of all books in the collection which match the given parameters.
            
        Parameters :
          - p_coll : the collection
          - p_from_date : starting date
          - p_to_date : ending date
          - p_l_list_id : list of lists id's.
    
        Return : the pandas dataframe
        """
        # The lists id's list may be None, which means that we need not filter the lists.
        if p_l_list_id is not None:
            _ppl_starter = [
                {
                    "$match": {
                        "$and": [
                            {
                                "published_date": {
                                "$gte": f"{p_from_date}",
                                "$lt": f"{p_to_date}",
                                }
                            },
                            { "list_id": { "$in": p_l_list_id } },
                        ]
                    }
                }
            ]
        else:
            _ppl_starter = [
                # Filter on dates only
                {
                    "$match": {
                        "published_date": {
                            "$gte": f"{p_from_date}",
                            "$lt": f"{p_to_date}",
                        },
                    }
                }
            ]
        
        # Aggregation pipeline
        _ppl = _ppl_starter + [
            # Get the date in real format
            {
                "$set": {
                    "publish_real_date": {
                        "$dateFromString": {
                            "dateString": "$published_date",
                            "format": "%Y-%m-%d",
                        }
                    }
                }
            },
            # Now get the year, month, day, week
            {
                "$set": {
                    "publish_year": {"$year": "$publish_real_date"},
                    "publish_month": {"$month": "$publish_real_date"},
                    "publish_day": {"$dayOfMonth": "$publish_real_date"},
                    "publish_week": {"$week": "$publish_real_date"},
                }
            },
            # Unwind the list book array
            {
                "$unwind": {
                    "path": "$books",
                    "preserveNullAndEmptyArrays": False
                }
            },
            # Select the required/useful fields
            {
                "$project": {
                    "_id": 0,
                    "list_id": "$list_id",
                    "list_name": "$list_name",
                    "list_encoded": "$list_name_encoded",
                    "list_display": "$display_name",
                    "title": "$books.title",
                    "author": "$books.author",
                    "publisher": "$books.publisher",
                    "publish_year": "$publish_year",
                    "publish_month": "$publish_month",
                    "publish_day": "$publish_day",
                    "publish_week": "$publish_week",
                    "isbn10": "$books.primary_isbn10",
                    "isbn13": "$books.primary_isbn13",
                    "amzn_lnk": "$books.amazon_product_url",
                    "description": "$books.description",
                    "rank": "$books.rank",
                    "rank_last_week": "$books.rank_last_week",
                    "rank_nb_weeks": "$books.weeks_on_list",
                    "image": "$books.book_image",
                    "image_w": "$books.book_image_width",
                    "image_h": "$books.book_image_height",
                }
            },
            {
                "$group": {
                    "_id": {
                        "author": "$author",
                        "title": "$title",
                        "publisher": "$publisher",
                        "isbn10": "$isbn10",
                        "isbn13": "$isbn13",
                        "rank": "$rank",
                    },
                    "count": {"$sum": 1},
                    "publish_year": {"$first": "$publish_year"},
                    "publish_month": {"$first": "$publish_month"},
                    "publish_day": {"$first": "$publish_day"},
                    "publish_week": {"$first": "$publish_week"},
                    "description": {"$first": "$description"},
                    "image": {"$last": "$image"},
                    "image_w": {"$last": "$image_w"},
                    "image_h": {"$last": "$image_h"},
                    "amzn_lnk": {"$last": "$amzn_lnk"},
                    "lists": {"$addToSet": "$list_id"},
                    "list_names": {"$addToSet": "$list_name"},
                }
            },
        ]

        logging.info(f"New pipeline : {_ppl}")
    
        # Run the aggregation ...
        _res = p_coll.aggregate(_ppl)
    
        # ... then convert results into a dataFrame ...
        _df = pd.json_normalize(list(_res))
    
        # ... then change the columns name :
        _df.columns = [x.replace("_id.", "") for x in _df.columns]
        
        return _df
        
    ##
    # Prices queries
    
    @staticmethod
    def prices_get_batch(p_coll:Collection, p_l_isbn10) -> pd.DataFrame :
        """
        Get the prices for all ISBN10 in the list.
        
        Parameters :
          - p_coll : the collection
          - p_l_isbn10 : list of ISBN10
        
        Return :
          - a dataframe with two columns (isbn10, price)
        """
        # Find the entries
        _results = list(p_coll.find(
            { "isbn10": { "$in": p_l_isbn10 } },
            { "_id":0, "isbn10":1, "price":1 }
        ))
        
        # Returns a dataframe
        return pd.DataFrame(_results)

    @staticmethod
    def prices_all_isbn(p_coll:Collection, p_country:str, p_oldest: str) -> list[str] :
        """
        Get the list of all ISBN10 that are currently in the collection, when their
        last update is not "too old" (i.e. earlier than the given date)
        
        Parameters :
          - p_coll : the collection
          - p_country : the country code of the web site we intend to use
          - p_oldest : the oldest admissible date (format : %Y-%m-%d)
        
        Return :
          - the list of ISBN10
        """
        # Here the request is quite straightforward :
        _conditions = {
            "country": p_country,
            "last_update": { "$gte": p_oldest }
        }

        return p_coll.distinct("isbn10", _conditions)

    @staticmethod
    def prices_update_price(p_coll:Collection, p_isbn10:str,
                            p_price:str, p_country:str,
                            p_date: str) -> None :
        """
        Update the price of the given ISBN10. If there's no entry, a new one is
        created.
        
        Parameters :
          - p_coll : the collection
          - p_isbn10 : the priced ISBN10
          - p_price : the price as a string (with the currency)
          - p_country : the country code of the web site used to get the price
          - p_date : the date of the update (format : %Y-%m-%d)
        
        Return :
          - Nothing
        """
        logging.info(f"Upserting : ISBN10 = {p_isbn10}")

        _res = p_coll.update_one(
            { "isbn10": p_isbn10},
            { "$set": {
                    "price": p_price,
                    "last_update": p_date,
                    "country": p_country,
                }
            },
            upsert=True
        )
        
        if not _res.acknowledged:
            logging.warning(f"Something fishy happened during the 'upsertion'")

        return
