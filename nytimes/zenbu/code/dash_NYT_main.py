# ============================================================================
#
# File    : dash_NYT.py
# Date    : 2024/09/09
# (c)     : Michaël Abergel - Alfred Quitman - Emmanuel Bompard
# Object  : Show dashboards based on NYT MongoDB database.
#           This script simply instantiate a main Dash layout
#           and a project script class
# Version : 0.3.0
#
# ============================================================================

##
# Basic imports (Python Library)
#
import os
import logging
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

# ============================================================================

###
# Instanciate Dash app at first
nyt_app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.BOOTSTRAP])

####
# Main

def main():
    """
    Main. :-)
    """
    # Gets the script object
    script = NYTScript(os.environ["NYT_CONFIG_FILE"])

    # Navigation bar : allow to switch from a page to another
    navbar = dbc.NavbarSimple(
        children=[
            # Our pages and their path
            dbc.NavItem(dbc.NavLink("Articles", href="/articles")),
            dbc.NavItem(dbc.NavLink("Books", href="/books")),
        ],
        brand="Browsing NYT API",
        brand_href="/articles",
        color="primary",
        dark=True,
        expand="md",
    )

    # Layout
    nyt_app.layout = html.Div([
        dcc.Location(id="url"),
        # Navigation bar
        navbar,
        # Where to put some content
        dash.page_container
    ])
    
    # Import the pages
    # import pages.dash_NYT_archives

    # Run Dash
    nyt_app.run_server(debug=False, host="0.0.0.0", port=8050)

    # That's all, folks ! Release the lock then exit.
    script.script_exit()

if __name__ == '__main__':
    main()
