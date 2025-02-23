# ============================================================================
#
# File    : dash_NYT_archives.py
# Date    : 2024/09/09
# (c)     : Michaël Abergel - Alfred Quitman - Emmanuel Bompard
# Object  : Show dashboards for archives/articles.
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


##
# MongoDB, Pandas etc.
from pymongo import MongoClient
import pandas as pd
import numpy as np

##
# Dash and graphics
import dash
from dash import Dash, dcc, callback, html, Input, Output, State, dash_table
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
if not hasattr(script, "nyt_arch"):
    script.nyt_arch = dict()

# ============================================================================
#
# Functions

###
# Dashboarding

def fig_article_month(p_from_date, p_to_date):
    """
    Create a diagram for the Article/Month query
    """
    _data_key = "_".join([
        str(x) for x in [currentframe().f_code.co_name, p_from_date, p_to_date]
    ])
    
    # Gets the dataframe from cache or from a query if there is no cache
    if not _data_key in script.nyt_arch:
        _df_res = NYTDBQueries.count_by_month(
            script.db["Archives"], [],
            p_from_date, p_to_date,
        )

        # Build year/month var from year and month
        _df_res["year_month"] = _df_res.apply(NYTDBQueries.year_month, axis=1)
    
        # Sort the results by the date.
        _df_res = _df_res.sort_values(by="year_month", ascending=True)
        
        # Store the results
        script.nyt_arch[_data_key] = _df_res
    else:
        _df_res = script.nyt_arch[_data_key]

    # Simple figure : bar chart
    _fig = px.bar(
        _df_res, x="year_month", y="count",
        text="count",
        labels={ "year_month": "Period", "count": "Nb. Articles" }
    )

    _fig.update_layout(
        title={
            'text': "Articles / Month",
            'y':0.95, 'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top',
        },
    )

    return _fig

def fig_article_section_month(p_from_date, p_to_date):
    """
    Create a diagram for the Article/Section/Month query
    """
    _data_key = "_".join([
        str(x) for x in [currentframe().f_code.co_name, p_from_date, p_to_date]
    ])
    
    # Gets the dataframe from cache or from a query if there is no cache
    if not _data_key in script.nyt_arch:
        # Gets the dataframe from the query
        _df_res = NYTDBQueries.count_by_month(
            script.db["Archives"], "section_name",
            p_from_date, p_to_date,
        )
    
        # Build year/month var from year and month
        _df_res["year_month"] = _df_res.apply(NYTDBQueries.year_month, axis=1)
    
        # Get the 10 most important sections
        _l_sections = (
            _df_res.groupby("section_name", as_index=False).agg({"count": "sum"})
            .sort_values(by="count", ascending=False)
            .head(10)
        )["section_name"].to_list()
    
        # Limit to these 10 sections
        _df_res = (
            _df_res[_df_res["section_name"].isin(_l_sections)]
            .sort_values(by="year_month", ascending=True)
        )
        
        # Store the results
        script.nyt_arch[_data_key] = _df_res
    else:
        _df_res = script.nyt_arch[_data_key]

    # Scatter plot
    _fig = px.scatter(
        _df_res, x="year_month", y="section_name",
        size="count", color="count",
        labels={ "year_month": "Period",
                 "section_name": "Section",
                 "count": "Nb. Articles" }
    )

    _fig.update_layout(
        title={
            'text': "Ten most important sections by month",
            'y':0.95, 'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top',
        },
    )

    return _fig

def fig_top_kw_year(p_from_date, p_to_date):
    """
    Creates a diagram showing the main keywords by month.
    """
    _data_key = "_".join([
        str(x) for x in [currentframe().f_code.co_name, p_from_date, p_to_date]
    ])

    # Gets the dataframe from cache or from a query if there is no cache
    if not _data_key in script.nyt_arch: 
        # Gets the dataframe from the query
        _df_res = NYTDBQueries.count_arch_keywords(
            script.db["Archives"],
            p_from_date, p_to_date,
        )
    
        # Build year/month var from year and month
        _df_res["year_month"] = _df_res.apply(NYTDBQueries.year_month, axis=1)
    
        # Sort the results by the date and frequency (count).
        _df_res = _df_res.sort_values(by=["year_month", "count"], ascending=True)
    
        # Aggregates all dates, sort and take the twenty most important.
        _gby = (
            _df_res.groupby(by=["keyword"], as_index=False).agg({ "count": "sum"})
            .sort_values(by="count", ascending=False)
            .head(7)
        )
        
        # Store the results
        script.nyt_arch[_data_key] = _gby
    else:
        _gby = script.nyt_arch[_data_key]

    # Simple figure : pie chart
    _fig = px.pie(
        _gby,
        values='count', names='keyword',
    )

    _fig.update_traces(textposition='inside')

    _fig.update_layout(
        uniformtext_minsize=12,
        uniformtext_mode='hide',
        title={
            'text': "Five most important keywords",
            'y':0.95, 'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top',
        },
    )

    return _fig

def fig_kw_freq_ranking(p_from_date, p_to_date):
    """
    Creates a diagram showing the frequency and average ranking
    of keywords for a given year ($$$$ a given month would also
    be fine)
    """
    _data_key = "_".join([
        str(x) for x in [currentframe().f_code.co_name, p_from_date, p_to_date]
    ])
    
    # Gets the dataframe from cache or from a query if there is no cache
    if not _data_key in script.nyt_arch: 
        # Gets the dataframe from the query
        _df_res = NYTDBQueries.count_arch_keywords(
            script.db["Archives"],
            p_from_date, p_to_date,
        )
    
        # Build year/month var from year and month
        _df_res["year_month"] = _df_res.apply(NYTDBQueries.year_month, axis=1)
    
        # Sort the results by the date and frequency (count).
        _df_res = _df_res.sort_values(by=["year_month", "count"], ascending=True)
    
        # Aggregates the keywords : counts the number of occurrences and the
        # weighted arithmetic mean of the ranking.
        _gby = (
            _df_res.groupby(by=["keyword", "kind"], as_index=False)
            .agg(
                count=("count", "sum"),
                avg_wght_rank=(
                    "rank",
                    lambda x: np.average(x, weights=_df_res.loc[x.index, "count"])
                )
             )
            .sort_values(by="count", ascending=False)
            .head(20)
        )
    
        # We need an inversed average rank so that the dot size are fine
        # (the lesser rank, the better)
        _gby["inverted_mean_rank"] = _gby["avg_wght_rank"].apply(lambda x: 1/(x + 1))
        
        # Store the results
        script.nyt_arch[_data_key] = _gby
    else:
        _gby = script.nyt_arch[_data_key]

    # Scatter plot with the keywords
    _fig = px.scatter(
        _gby,
        x="count", y="avg_wght_rank",
        text="keyword",
        color="kind",
        size="inverted_mean_rank",
        labels={ "count": "Occurrences",
                 "avg_wght_rank": "Average rank",
                 "kind": "Keyword type" },
    )

    _fig.update_traces(
        textposition='top center',
        hovertemplate =
        "<b>Keyword</b>: %{text}<br>" +
        # "<b>Keyword type</b>: %{kind}<br>" +
        "<b>Avg. Rank</b>: %{y:.1f}<br>" +
        "<b>Occurrences</b>: %{x}",
    )

    _fig.update_layout(
        title={
            'text': "Top keyword frequency / ranking for the period",
            'y':0.95, 'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top',
        },
    )

    return _fig

def fig_kw_cloud(p_from_date, p_to_date):
    """
    Creates a word cloud from the keyword list.
    """
    _data_key = "_".join([
        str(x) for x in [currentframe().f_code.co_name, p_from_date, p_to_date]
    ])
    
    # Gets the dataframe from cache or from a query if there is no cache
    if not _data_key in script.nyt_arch: 
        # Gets the dataframe from the query
        _df_res = NYTDBQueries.count_arch_keywords(
            script.db["Archives"],
            p_from_date, p_to_date,
        )
    
        # Build year/month var from year and month
        _df_res["year_month"] = _df_res.apply(NYTDBQueries.year_month, axis=1)
    
        # Sort the results by the date and frequency (count).
        _df_res = _df_res.sort_values(by=["year_month", "count"], ascending=True)
    
        # Counts the number of occurrences for each keyword.
        _gby = (
            _df_res.groupby(by=["keyword"], as_index=False)
            .agg(
                count=("count", "sum"),
            )
            .sort_values(by="count", ascending=False)
            .head(50)
        )
    
        # Concatenate the keyword as a text
        _text = " ".join(_gby["keyword"].to_list())
    
        # Get the stop words :
        if not hasattr(script, "_stop_words"):
            nltk.download('stopwords')
            script._stop_words = stopwords.words('english')
    
        _word_cloud = WordCloud(
            background_color = 'white',
            stopwords = script._stop_words,
            height=300,
            width=500,
            max_words = 50
        ).generate(_text)
        
        # Store the results
        script.nyt_arch[_data_key] = _word_cloud
    else:
        _word_cloud = script.nyt_arch[_data_key]

    # Create figure
    _fig = px.imshow(_word_cloud)

    # Add a title
    _fig.update_layout(
        title={
            'text': "Keywords cloud for the selected year(s)",
            'y':0.95, 'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top',
        },
    )

    # Remove the axis :
    _fig.update_xaxes(showticklabels=False, visible=False)
    _fig.update_yaxes(showticklabels=False, visible=False)

    return _fig

###
# Callbacks

@callback(
    Output('arch_from_year', 'value'),
    [Input('arch_from_year', 'value')]
)
def arch_enforce_from_year(p_value):
    """
    Callback for the "year from" selection. Enforce a value to be selected.
    """
    if p_value is None:
        return str(script.nyt_arch["year_min"])
    return dash.no_update

@callback(
    Output('arch_from_month', 'value'),
    [Input('arch_from_month', 'value')]
)
def arch_enforce_from_month(p_value):
    """
    Callback for the "month from" selection. Enforce a value to be selected.
    """
    if p_value is None:
        return "01"
    return dash.no_update

@callback(
    Output('arch_to_year', 'value'),
    [Input('arch_to_year', 'value')]
)
def arch_enforce_to_year(p_value):
    """
    Callback for the "year to" selection. Enforce a value to be selected.
    """
    if p_value is None:
        return str(script.nyt_arch["year_max"])
    return dash.no_update

@callback(
    Output('arch_to_month', 'value'),
    [Input('arch_to_month', 'value')]
)
def arch_enforce_to_month(p_value):
    """
    Callback for the "month from" selection. Enforce a value to be selected.
    """
    if p_value is None:
        return "12"
    return dash.no_update

@callback(
    Output('arch_NYT_graph', 'figure'),
    [Input("arch_query_radio", "value"),
     Input("arch_from_year", "value"),
     Input("arch_from_month", "value"),
     Input("arch_to_year", "value"),
     Input("arch_to_month", "value"),]
)
def arch_update_figure(p_query_radio,
                       p_from_year, p_from_month,
                       p_to_year, p_to_month):
    """
    Create a new diagram according to the state of the objects (i.e. sliders,
    radio buttons and the like).
    Or create a new image (in case of the word cloud)

    The following objects are used and their value are provided as parameters :
      - arch_query_radio : to choose amongst the available queries
      - p_from_year : year of beginning of query
      - p_from_month : month of year of beginning of query
      - p_to_year : year of end of query
      - p_to_month : month of year of end of query

    Returns the figure.
    """
    print(f"{p_query_radio = }, {p_from_year = }, {p_from_month = }, {p_to_year = }, {p_to_month = }")

    # Get Dash context
    ctx = dash.callback_context

    # If something triggered the callback, get its id (for now, it's
    # just for information)
    if ctx.triggered:
        _obj_id = ctx.triggered[0]['prop_id'].split('.')[0]
        logging.info(f"{_obj_id = }")

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

    # Default return values
    _fig = go.Figure()

    match p_query_radio:
        case "articles_month":
            _fig = fig_article_month(_from_date, _to_date)
        case "article_section_month":
            _fig = fig_article_section_month(_from_date, _to_date)
        case "top_kw_year":
            _fig = fig_top_kw_year(_from_date, _to_date)
        case "top_kw_freq_rank":
            _fig = fig_kw_freq_ranking(_from_date, _to_date)
        case "kw_cloud":
            _fig = fig_kw_cloud(_from_date, _to_date)
        case _:
            pass

    return _fig

# ============================================================================
#
# "main" part of the script

# Get the current min and max year for the archive collection
# (if not already known)
if not "year_min" in script.nyt_arch:
    script.nyt_arch["year_min"], script.nyt_arch["year_max"] = NYTDBQueries.year_limits(script.db["Archives"])

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

# Register the page
dash.register_page(__name__, path="/articles")

# Layout
layout = html.Div([
    # Header
    html.Div([
        html.H1(
            "NYT MongoDB Queries of Archives",
            style={'textAlign': 'center', 'color': '#333'}
            ),
        html.Hr(style={'border': '1px solid #bbb'})
    ], style={'marginBottom': '40px'}),

    # Main tools
    html.Div([
        html.H2("Collection : Archives / Articles",
                style={ 'marginBottom': '20px', 'textAlign': 'center' }),

        html.Div([
            # Choose the query
            html.Div([
                html.H3("Queries:"),
                dcc.RadioItems(
                    id="arch_query_radio",
                    options=[
                       {"label": " Articles / Month", "value": "articles_month"},
                       # {"label": "Articles / Section / Month", "value": "article_section_month"},
                       {"label": " Top keywords", "value": "top_kw_year"},
                       {"label": " Top keywords frequency / ranking", "value": "top_kw_freq_rank"},
                       {"label": " Keywords cloud", "value": "kw_cloud"},
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
            ], style={'width': '80%', 'margin': 'auto', 'justify-content': 'right'}),

            # Choose the dates : from / to ; year / month
            html.Div([
                html.Div([
                    html.H5("Date from:", style={'marginBottom': '5px'}),
                    dcc.Dropdown(
                        id="arch_from_year",
                        options=[str(y) for y in range(script.nyt_arch["year_min"],
                                                       script.nyt_arch["year_max"] + 1)],
                        value=str(script.nyt_arch["year_min"]),
                        persistence=True,
                        persistence_type='local',
                    ),
                    dcc.Dropdown(
                        id="arch_from_month",
                        options=l_months_options,
                        value="01",
                        persistence=True,
                        persistence_type='local',
                    ),
                ], style={'width': '45%', 'margin': 'auto', 'display': 'inline-block', 'margin-right': '5%' }),
                
                html.Div([
                    html.H5("Date to:", style={'marginBottom': '5px'}),
                    dcc.Dropdown(
                        id="arch_to_year",
                        options=[str(y) for y in range(script.nyt_arch["year_min"],
                                                       script.nyt_arch["year_max"] + 1)],
                        value=str(script.nyt_arch["year_max"]),
                        persistence=True,
                        persistence_type='local',
                    ),
                    dcc.Dropdown(
                        id="arch_to_month",
                        options=l_months_options,
                        value="01",
                        persistence=True,
                        persistence_type='local',
                    ),
                ], style={'width': '45%', 'margin': 'auto', 'display': 'inline-block', 'margin-right': '5%' }),
            ], style={'width': '95%', 'margin': 'auto', 'display': 'inline-block', 'margin-right': '5%' },
               id="arch_date_selection_container"),
        ], style={'display': 'grid', 'gridTemplateColumns': '1fr 1fr'} ),

    ], style={'padding': '20px', 'textAlign': 'left',
              'backgroundColor': '#f9f9f9', 'borderRadius': '10px'}),

    html.Hr(style={'border': '1px solid #bbb'}),
    html.H2("Diagram", style={'textAlign': 'center', 'color': '#333'}),
    # Graphics
    html.Div([
        # The graphs
        dcc.Graph(id='arch_NYT_graph'),
    ], style={'width': '80%',
              'margin': '0 auto',
              'textAlign': 'center',
              'marginBottom': '40px'}),
], style={ 'width': '100%', "font-family": "sans-serif" })
