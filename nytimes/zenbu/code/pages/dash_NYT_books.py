# ============================================================================
#
# File    : dash_NYT_books.py
# Date    : 2024/09/09
# (c)     : Michaël Abergel - Alfred Quitman - Emmanuel Bompard
# Object  : Show dashboards for books / best sellers.
# Version : 0.2.0
#
# ============================================================================

##
# Basic imports (Python Library)
#
import os
import logging
from inspect import currentframe
import json
from html import escape


##
# MongoDB, Pandas etc.
from pymongo import MongoClient
import pandas as pd
import numpy as np

##
# Dash and graphics
import dash
from dash import Dash, dcc, callback, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go

##
# Word cloud
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from PIL import Image
import base64
from io import BytesIO

# To get stop words
import nltk
from nltk.corpus import stopwords

##
# Project imports
from nyt_utils.nyt_script import NYTErr, NYTScript
from nyt_utils.nyt_dbqueries import NYTDBQueries

# ============================================================================

# Get the script instance
script = NYTScript(os.environ["NYT_CONFIG_FILE"])

# Set the context for this page
if not hasattr(script, "nyt_books"):
    script.nyt_books = dict()

# ============================================================================
#
# Simple functions

def image_to_img(p_row):
    """
    Take the three columns describing an image and yield an HTML string
    describing an image. 
    """
    # return f"""<img src="{p_row["image"]}" style="height: 50px; width: 33px;" />"""
    _esc_desc = escape(p_row["description"]) if p_row["description"] is not None else ""
    _s = f"""<a href="{p_row["amzn_lnk"]}" target="_blank"><img src="{p_row["image"]}" style="height: 50px; width: 33px;" alt="{_esc_desc}"/></a>"""
    return _s

def list_to_string(p_l):
    """
    Make a string of a list of things. Suppose each thing can be converted
    into a string.
    """
    return "None" if p_l is None else "_".join(map(str, p_l))


###
# Dashboarding

def fig_best_publisher_rank(p_from_date, p_to_date, p_l_list_id):
    """
    Creates a diagram showing the most successful publishers for the
    given parameters.
    """
    _data_key = "_".join([
        str(x) for x in [currentframe().f_code.co_name,
                         p_from_date, p_to_date, list_to_string(p_l_list_id)]
    ])
    
    # Gets the dataframe from cache or from a query if there is no cache
    if not _data_key in script.nyt_books: 
        # Gets the dataframe from the query
        _df_res = NYTDBQueries.list_books(script.db["Books"], p_from_date,
                                        p_to_date, p_l_list_id)
    
        # The query may not yield any output :
        if (_df_res is None) or (_df_res.shape[0] == 0):
            return None
    
        # Creates an inverted rank (it will simplify the weighted mean
        # hereafter :
        _df_res["u_rank"] = _df_res["rank"].apply(lambda x: 1/(x + 1))
    
        # First aggregation, to get counts and average ranks for publisher/books.
        _gby_one = (
            _df_res.groupby(by=["publisher", "isbn13"], as_index=False)
            .agg(
                count=("count", "sum"),
                avg_wght_rank=(
                    "u_rank",
                    lambda x: np.average(x, weights=_df_res.loc[x.index, "count"])
                )
            )
        )
        
        # Then second aggregation, to get counts and average ranks for publishers
        _gby = (
            _gby_one.groupby(by=["publisher"], as_index=False)
            .agg(
                total=("isbn13", "count"),
                avg_wght_rank=(
                    "avg_wght_rank",
                    lambda x: np.average(x, weights=_df_res.loc[x.index, "count"])
                )
            )
        )
        
        # Store the results
        script.nyt_books[_data_key] = _gby
    else:
        _gby = script.nyt_books[_data_key]

    # Simple figure : bar chart
    _fig = px.bar(
        _gby.sort_values(by=["total"], ascending=False).head(10),
        x="publisher", y="total",
        text="total",
        color="avg_wght_rank",
        labels={ "publisher": "Publishers",
                 "total": "Books",
                 "avg_wght_rank": "Weighted Ranks\n(higher's best)" }
    )

    _fig.update_layout(
        title={
            'text': f"Ten Best Publishers for the selected dates and lists",
            'y':0.95, 'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top',
        },
    )

    return _fig

def fig_best_author_rank(p_from_date, p_to_date, p_l_list_id):
    """
    Creates a diagram showing the most successful authors for the
    given parameters.
    """
    _data_key = "_".join([
        str(x) for x in [currentframe().f_code.co_name,
                         p_from_date, p_to_date, list_to_string(p_l_list_id)]
    ])
    
    # Gets the dataframe from cache or from a query if there is no cache
    if not _data_key in script.nyt_books: 
        # Gets the dataframe from the query
        _df_res = NYTDBQueries.list_books(script.db["Books"], p_from_date,
                                        p_to_date, p_l_list_id)
    
        # The query may not yield any output :
        if (_df_res is None) or (_df_res.shape[0] == 0):
            return None
    
        # Creates an inverted rank (it will simplify the weighted mean
        # hereafter :
        _df_res["u_rank"] = _df_res["rank"].apply(lambda x: 1/(x + 1))
    
        # First aggregation, to get counts and average ranks for author/books.
        _gby_one = (
            _df_res.groupby(by=["author", "isbn13"], as_index=False)
            .agg(
                count=("count", "sum"),
                avg_wght_rank=(
                    "u_rank",
                    lambda x: np.average(x, weights=_df_res.loc[x.index, "count"])
                )
            )
        )
        
        # Then second aggregation, to get counts and average ranks for authors
        _gby = (
            _gby_one.groupby(by=["author"], as_index=False)
            .agg(
                total=("isbn13", "count"),
                avg_wght_rank=(
                    "avg_wght_rank",
                    lambda x: np.average(x, weights=_df_res.loc[x.index, "count"])
                )
            )
        )
        
        # Store the results
        script.nyt_books[_data_key] = _gby
    else:
        _gby = script.nyt_books[_data_key]

    # Simple figure : bar chart
    _fig = px.bar(
        _gby.sort_values(by=["total"], ascending=False).head(10),
        x="author", y="total",
        text="total",
        color="avg_wght_rank",
        labels={ "author": "Authors",
                 "total": "Books",
                 "avg_wght_rank": "Weighted Ranks\n(higher's best)" }
    )

    _fig.update_layout(
        title={
            'text': f"Ten Best Authors for the selected dates and lists",
            'y':0.95, 'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top',
        },
    )

    return _fig

def table_best_books_rank(p_from_date, p_to_date, p_l_list_id):
    """
    Creates a table with the best ranking books, for the given parameters.
    """
    _data_key = "_".join([
        str(x) for x in [currentframe().f_code.co_name,
                         p_from_date, p_to_date, list_to_string(p_l_list_id)]
    ])
    
    # Gets the dataframe from cache or from a query if there is no cache
    if not _data_key in script.nyt_books: 
        # Gets the dataframe from the query
        _df_res = NYTDBQueries.list_books(script.db["Books"], p_from_date,
                                          p_to_date, p_l_list_id)
        
        # The query may not yield any output :
        if (_df_res is None) or (_df_res.shape[0] == 0):
            return None, None, None
    
        # Creates an inverted rank (it will simplify the weighted mean
        # hereafter :
        _df_res["u_rank"] = _df_res["rank"].apply(lambda x: 1/(x + 1))
    
        # Aggregation by book (here : ISBN13) : we compute the average ranking of
        # the book, the number of times it was listed and keep its title, author
        # and image.
        _gby = (
            _df_res.groupby(by=["isbn10"], as_index=False)
            .agg(
                total=("count", "sum"),
                avg_wght_rank=(
                    "u_rank",
                    lambda x: np.average(x, weights=_df_res.loc[x.index, "count"])
                ),
                image=("image", "last"),
                image_w=("image_w", "last"),
                image_h=("image_h", "last"),
                amzn_lnk=("amzn_lnk", "last"),
                author=("author", "first"),
                title=("title", "first"),
                description=("description", "first"),
            )
        )
        
        # Create a new column with the HTML code for the image
        _gby["html_img"] = _gby.apply(image_to_img, axis=1)
        
        # Create a new column for the price. Empty at first
        # _gby["price"] = "N/A"

        # Order by "total" and keep only 50 first values
        _gby = _gby.sort_values(by="total", ascending=False)[[
            "isbn10", "html_img", "author", "title", 
            "total", "description",
        ]].head(50)
        
        # Get the price for the ISBN
        _df = NYTDBQueries.prices_get_batch(
            script.db[script.d_config["prices"]["coll_name"]],
            _gby["isbn10"].to_list()
        )
        
        # Set the price in the main dataframe.
        _gby = pd.merge(_gby, _df, on="isbn10", how="left")

        # Store the results
        script.nyt_books[_data_key] = _gby
    else:
        _gby = script.nyt_books[_data_key]

    # Define the columns
    _columns = [
        {'name': 'ISBN10', "id": "isbn10"},
        {"name": "Icon", "id": "html_img", "presentation": "markdown"},
        {"name": "Author", "id": "author"},
        {"name": "Title", "id": "title"},
        {"name": "Occurr.", "id": "total"},
        {"name": "Description", "id": "description" },
        {"name": "Price", "id": "price" },
    ]

    # Convert the dataframe to a list of columns.
    _table = _gby[[
        "isbn10", "html_img", "author", "title", 
        "total", "description", "price"
    ]].to_dict('records')
    
    _tooltip_data=[
        {
            k: row["description"] for k in row.keys()
        } for row in _table
    ]

    return _table, _columns, _tooltip_data

###
# Callbacks

@callback(
    Output('books_from_year', 'value'),
    [Input('books_from_year', 'value')]
)
def books_enforce_from_year(p_value):
    """
    Callback for the "year from" selection. Enforce a value to be selected.
    """
    if p_value is None:
        return str(script.nyt_books["year_min"])
    return dash.no_update

@callback(
    Output('books_from_month', 'value'),
    [Input('books_from_month', 'value')]
)
def books_enforce_from_month(p_value):
    """
    Callback for the "month from" selection. Enforce a value to be selected.
    """
    if p_value is None:
        return "01"
    return dash.no_update

@callback(
    Output('books_to_year', 'value'),
    [Input('books_to_year', 'value')]
)
def books_enforce_to_year(p_value):
    """
    Callback for the "year to" selection. Enforce a value to be selected.
    """
    if p_value is None:
        return str(script.nyt_books["year_max"])
    return dash.no_update

@callback(
    Output('books_to_month', 'value'),
    [Input('books_to_month', 'value')]
)
def books_enforce_to_month(p_value):
    """
    Callback for the "month from" selection. Enforce a value to be selected.
    """
    if p_value is None:
        return "12"
    return dash.no_update

# Callback to update a cell value
# @callback(
#     Output('books_NYT_table', 'data'),
#     [Input('books_get_price_button', 'n_clicks'),
#      Input('books_NYT_table', 'derived_viewport_data')]
# )
# def books_get_price(p_n_clicks, p_viewport_data):
#     """
#     Callback for the "get price" button.
#     """
#     logging.info(f"{p_n_clicks = }")
#     logging.info(f"{p_viewport_data = }")
#     
#     if p_n_clicks is None:
#         # No price request yet
#         return dash.no_update
#     
#     # Update 'Status' column for rows currently displayed
#     # updated_data = data.copy()
#     if p_viewport_data:
#         visible_ids = [row['ID'] for row in p_viewport_data]
#         print(visible_ids)
#         # updated_data.loc[updated_data['ID'].isin(visible_ids), 'Status'] = 'Updated'
#     
#     return dash.no_update

@callback(
    [Input('books_get_price_button', 'n_clicks')]
)
def book_get_price(p_n_clicks):
    """
    Callback for the "get price" button.
    """
    logging.info(f"{p_n_clicks = }")

    return

@callback(
    [Output('books_graph_container', 'style'),
     Output('books_table_container', 'style')],
    [Input('books_query_radio', 'value')]
)
def books_show_hide(p_query_radio):
    """
    Change the style of both graph and table according to the selected
    query.

    Three objects are used and their value are provided as parameters :
      - p_query_radio : to choose amongst the available queries

    Returns graph and table styles.
    """
    print(f"{p_query_radio = }")
    
    # Styles hide/show
    _style_hide = {'display': 'none'}
    _style_show = {'display': 'block'}
    
    # Default return values
    _style_table = _style_hide
    _style_graph = _style_hide
    
    match p_query_radio:
        case "books_auth_list":
            _style_table = _style_hide
            _style_graph = _style_show 
        case "books_publi_list":
            _style_table = _style_hide
            _style_graph = _style_show 
        case "books_list_period":
            _style_table = _style_show
            _style_graph = _style_hide 
        case _:
            pass

    return _style_graph, _style_table


@callback(
    [Output('books_NYT_graph', 'figure'),
     Output('books_NYT_table', 'data'),
     Output('books_NYT_table_h2', 'children'),
     Output('books_NYT_table', 'columns'),
     Output('books_NYT_table', 'tooltip_data')],
    [Input('books_query_radio', 'value'),
     Input("books_from_year", "value"),
     Input("books_from_month", "value"),
     Input("books_to_year", "value"),
     Input("books_to_month", "value"),
     Input("books_lists_list", "value")]
)
def books_update_figure(p_query_radio, p_from_year, p_from_month,
                       p_to_year, p_to_month, p_lists_list):
    """
    Create a new diagram or table according to the state of the objects
    (i.e. sliders, radio buttons and the like).

    Three objects are used and their value are provided as parameters :
      - p_query_radio : to choose amongst the available queries
      - p_from_year : year of beginning of query
      - p_from_month : month of year of beginning of query
      - p_to_year : year of end of query
      - p_to_month : month of year of end of query
      - p_lists_list : list(s) to consider

    Returns both the figure and the image.
    """
    print(f"{p_query_radio = }, {p_from_year = }, {p_from_month = }")
    print(f"{p_to_year = }, {p_to_month = }, {p_lists_list = }")
    
    # Get Dash context
    ctx = dash.callback_context

    # If something triggered the callback, get its id (for now, it's
    # just for information)
    if ctx.triggered:
        _obj_id = ctx.triggered[0]['prop_id'].split('.')[0]
        logging.info(f"{_obj_id = }")

    # Default return values
    _fig = go.Figure()
    _table = [{"empty": "empty"}]
    _title = ""
    _columns = [{'empty': 'Empty'}]
    _tooltips = []
    
    # Build the date strings. Note that dates don't have to exist truly
    # but will be used simply as limits in a string comparison. If one
    # day we actually use dates, a more accurate function shall be used.
    _from_month = "01" if p_from_month is None else p_from_month
    _from_date = f"{p_from_year}-{p_from_month}-01"

    _to_month = "12" if p_to_month is None else p_to_month
    _to_date = f"{p_to_year}-{p_to_month}-{d_max_days[p_to_month]}"
    
    # If the user got it in the wrong direction, invert the dates...
    if (_from_date > _to_date):
        _to_date, _from_date = _from_date, _to_date
    
    # The lists list might actually be a single value -> convert it to a
    # list. But if "All" (value == -1) has been chosen, we use None since
    # it won't be a filter in the queries
    if (_lists_list := p_lists_list) is not None:        
        if not isinstance(_lists_list, list):
            _lists_list = [_lists_list]
        if ((-1 in _lists_list) or (len(_lists_list) == 0)):
            _lists_list = None

    match p_query_radio:
        case "books_auth_list":
            _new_fig = fig_best_author_rank(_from_date, _to_date, _lists_list)
            _fig = _new_fig if _new_fig is not None else _fig
        case "books_publi_list":
            _new_fig = fig_best_publisher_rank(_from_date, _to_date, _lists_list)
            _fig = _new_fig if _new_fig is not None else _fig
        case "books_list_period":
            _new_table, _new_columns, _new_tooltips = table_best_books_rank(_from_date, _to_date, _lists_list)
            if _new_table is not None:
                _table, _columns, _tooltips = _new_table, _new_columns, _new_tooltips
                _title = "Best Sellers for the selected period and lists"
        case _:
            pass

    return _fig, _table, _title, _columns, _tooltips

# ============================================================================
#
# "main" part of the script

# Get the current min and max year for the books collection
# (if not already known)

if not "year_min" in script.nyt_books:
    script.nyt_books["year_min"], script.nyt_books["year_max"] = (
        NYTDBQueries.year_limits(
            script.db["Books"],
            p_date_field="published_date",
            p_format="%Y-%m-%d",
        )
    )

# Get the list of lists (with their number of occurrences)
if not "list_lists" in script.nyt_books:
    script.nyt_books["list_lists"] = NYTDBQueries.list_lists(script.db["Books"])

# Hence the options list for the dropdown menu :
l_list_options = [ { "label": "All", "value": -1 } ] + [
    { "label": n, "value": v }
    for n, v in zip(
        script.nyt_books["list_lists"]["list_name"],
        script.nyt_books["list_lists"]["list_id"]
    )
]

# Month dictionary (for the dropdowns)
d_months = {
    "January": "01",
    "February": "02",
    "March": "03",
    "April": "04",
    "May": "05",
    "June": "06",
    "July": "07",
    "August": "08",
    "September": "09",
    "October": "10",
    "November": "11",
    "December": "12",
}
# Max days for each month.
# We don't bother with february and consider it 28 days long. It actually
# doesn't matter.
d_max_days = {
    "01": "31",
    "02": "28",
    "03": "31",
    "04": "30",
    "05": "31",
    "06": "30",
    "07": "31",
    "08": "31",
    "09": "30",
    "10": "31",
    "11": "30",
    "12": "31",
}
l_months_options = [ { "label": k, "value": v } for k,v in d_months.items() ]

# Register the page with a URL
dash.register_page(__name__, path="/books")

# Layout
layout = html.Div([
    # Header
    html.Div([
        html.H1(
            "Browsing Bestsellers lists",
            style={'textAlign': 'center', 'color': '#333'}
            ),
        html.Hr(style={'border': '1px solid #bbb'})
    ], style={'marginBottom': '40px'}),

    # Main tools
    html.Div([
        html.H2("Collection : Books",
                style={ 'marginBottom': '20px', 'textAlign': 'center' }),

        html.Div([
            # Choose the query
            html.Div([
                html.H3("Queries:"),
                dcc.RadioItems(
                    id="books_query_radio",
                    options=[
                       {"label": " Best Authors", "value": "books_auth_list"},
                       {"label": " Best Publishers", "value": "books_publi_list"},
                       {"label": " Best Books", "value": "books_list_period"},
                    ],
                    value="null",
                    style={
                        "display": "grid",
                        "grid-template-columns": "repeat(2, 1fr)",
                        "grid-gap": "5px",
                    },
                    persistence=True,
                    persistence_type='local',
                ),
            ], style={'width': '95%', 'margin': 'auto', 'display': 'inline-block', 'margin-right': '5%' },
               id="books_queries_container"),

            # Choose the dates : from / to ; year / month
            html.Div([
                html.Div([
                    html.H5("Date from:", style={'marginBottom': '5px'}),
                    dcc.Dropdown(
                        id="books_from_year",
                        options=[str(y) for y in range(script.nyt_books["year_min"],
                                                       script.nyt_books["year_max"] + 1)],
                        value=str(script.nyt_books["year_min"]),
                        persistence=True,
                        persistence_type='local',
                    ),
                    dcc.Dropdown(
                        id="books_from_month",
                        options=l_months_options,
                        value="01",
                        persistence=True,
                        persistence_type='local',
                    ),
                ], style={'width': '45%', 'margin': 'auto', 'display': 'inline-block', 'margin-right': '5%' }),
                
                html.Div([
                    html.H5("Date to:", style={'marginBottom': '5px'}),
                    dcc.Dropdown(
                        id="books_to_year",
                        options=[str(y) for y in range(script.nyt_books["year_min"],
                                                       script.nyt_books["year_max"] + 1)],
                        value=str(script.nyt_books["year_max"]),
                        persistence=True,
                        persistence_type='local',
                    ),
                    dcc.Dropdown(
                        id="books_to_month",
                        options=l_months_options,
                        value="01",
                        persistence=True,
                        persistence_type='local',
                    ),
                ], style={'width': '45%', 'margin': 'auto', 'display': 'inline-block', 'margin-right': '5%' }),
            ], style={'width': '95%', 'margin': 'auto', 'display': 'inline-block', 'margin-right': '5%' },
               id="books_date_selection_container"),

            # Choose the lists to explore & button to get the price
            html.Div([
                html.H5("List selection:", style={'marginBottom': '5px'}),
                html.Div([
                    dcc.Dropdown(
                        id="books_lists_list",
                        options=l_list_options,
                        value=-1,
                        multi=True,
                        persistence=True,
                        persistence_type='local',
                    ),
                ]),
            ], style={'width': '95%', 'display': 'inline-block', 'margin-top': '25px'},
               id="books_list_selection_container"),

        # ], style={'display': 'grid', 'gridTemplateColumns': '1fr 1fr 1fr' } ),
        ], style={'display': 'flex', 'flexDirection': 'row', 'alignItems': 'flex-start' }),

    ], style={'padding': '20px', 'textAlign': 'left',
              'backgroundColor': '#f9f9f9', 'borderRadius': '10px'}),

    html.Hr(style={'border': '1px solid #bbb'}),
    
    # Graphics and tables
    html.Div([
        html.Div([
            html.H2("Diagram", style={'textAlign': 'center', 'color': '#333'}),
            # The graph...
            dcc.Graph(id='books_NYT_graph'),            
        ], style={ 'display': 'block' }, id="books_graph_container"),
        # ... or the table
        html.Div([
            html.H2(
                "Table",
                id="books_NYT_table_h2",
                style={'textAlign': 'center', 'color': '#333'}
            ),
            dash_table.DataTable(
                id="books_NYT_table",
                markdown_options={ "html": True },
                data=[{"empty": "empty"}],
                page_size=5,
                tooltip_duration=None,
                style_header={
                    "textAlign": "center",
                    "fontWeight": "bold",
                    "fontFamily": "monospace",
                    "fontSize": "16px" 
                },
                style_cell={
                    "textAlign": "left",
                    "fontWeight": "roman",
                    "fontFamily": "monospace",
                    "fontSize": "14px" 
                },
                style_cell_conditional=[
                    {
                        "if": {"column_id": "total"},
                        "fontWeight": "bold",
                        "textAlign": "right",
                    },
                    {
                        "if": {"column_id": "description"},
                        "display": "none",
                    },
                    {
                        "if": {"column_id": "isbn10"},
                        "display": "none",
                    },
                    {
                        "if": {"column_id": "price"},
                        "textAlign": "center",
                    },
                ],
                css=[
                    {
                        # To center images
                        'selector': 'img', 
                        'rule': 'display: block; margin-left: auto; margin-right: auto; margin-top: auto; margin-bottom: auto;'
                    }
                ],
            ),
            # To display the Amazon link : does not work.
            dbc.Modal(
                [
                    dbc.ModalHeader("Book Details"),
                    dbc.ModalBody(id='books_modal_body'),
                    dbc.ModalFooter(
                        dbc.Button("Close", id="books_modal_close", className="ml-auto")
                    ),
                ],
                id="books_modal",
                is_open=False
            )

        ], style={ 'display': 'none' },
           id="books_table_container"),
    ], style={'width': '80%',
              'margin': '0 auto',
              'textAlign': 'center',
              'marginBottom': '40px'}),
], style={ 'width': '100%', "font-family": "sans-serif" })
