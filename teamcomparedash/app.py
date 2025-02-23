import dash
from dash import dcc, html, Input, Output, State
import pandas as pd
import plotly.express as px

# Load and filter the data
df = pd.read_csv('nba_2013.csv')

# Filter rows where 'bref_team_id' == 'TOT' and 'pos' == 'G'
df = df[(df['bref_team_id'] != 'TOT') & (df['pos'] != 'G')]

# Define player categories
rookies = df[df['age'] < 24]
seniors = df[df['age'] >= 24]

# Start the Dash application with suppress_callback_exceptions
app = dash.Dash(__name__, assets_folder="assets", suppress_callback_exceptions=True)

# Define the homepage layout
app.layout = html.Div([
    html.H1("NBA Comparative Dashboard"),
    
    # Button for player comparison
    html.Button('Player Comparison', id='btn-joueur', n_clicks=0),
    
    # Button for team comparison
    html.Button('Team Comparison', id='btn-equipe', n_clicks=0),
    
    # Dynamic content
    html.Div(id='page-content')
])

# Callback to manage page navigation
@app.callback(
    Output('page-content', 'children'),
    [Input('btn-joueur', 'n_clicks'),
     Input('btn-equipe', 'n_clicks')]
)
def display_page(btn_joueur, btn_equipe):
    # Display player comparison page if the player button is clicked
    if btn_joueur > 0:
        return joueur_comparatif_layout()
    # Display team comparison page if the team button is clicked
    elif btn_equipe > 0:
        return equipe_comparatif_layout()
    # Default message prompting user to select an option
    return html.Div("Select an option to begin.")

# Layout for player comparison page
def joueur_comparatif_layout():
    return html.Div([
        html.H2("Player Comparison"),
        
        # Dropdown to select rookie players
        dcc.Dropdown(
            id='rookie-dropdown',
            options=[{'label': i, 'value': i} for i in rookies['player'].unique()],
            placeholder="Select a rookie"
        ),
        
        # Dropdown to select senior players
        dcc.Dropdown(
            id='senior-dropdown',
            options=[{'label': i, 'value': i} for i in seniors['player'].unique()],
            placeholder="Select a senior"
        ),
        
        # Display player stats
        html.Div(id='joueur-stats')
    ])

# Callback to display player stats
@app.callback(
    Output('joueur-stats', 'children'),
    [Input('rookie-dropdown', 'value'),
     Input('senior-dropdown', 'value')]
)
def afficher_stats_joueurs(rookie, senior):

    # Check if both dropdowns have values
    if not rookie or not senior:
        return "Please select both a rookie and a senior."
    
    # If both a rookie and a senior are selected, display their stats
    if rookie and senior:
        rookie_stats = rookies[rookies['player'] == rookie].iloc[0]
        senior_stats = seniors[seniors['player'] == senior].iloc[0]
        
        # Display the stats as a table
        return html.Div([
            html.H3(f"Statistics for {rookie} (Rookie)"),
            html.Table([
                html.Tr([html.Th(col), html.Td(rookie_stats[col])]) for col in ['g', 'mp', 'pts', 'ast', 'trb', 'stl', 'blk']
            ]),
            html.H3(f"Statistics for {senior} (Senior)"),
            html.Table([
                html.Tr([html.Th(col), html.Td(senior_stats[col])]) for col in ['g', 'mp', 'pts', 'ast', 'trb', 'stl', 'blk']
            ])
        ])
    # If no selection, prompt the user to select players
    return "Please select both a rookie and a senior."

# Layout for team comparison page
def equipe_comparatif_layout():
    return html.Div([
        html.H2("Team Comparison"),
        
        # Dropdown to select statistics
        dcc.Dropdown(
            id='stat-dropdown',
            options=[
                {'label': 'Assists', 'value': 'ast'},
                {'label': 'Points', 'value': 'pts'},
                {'label': 'Rebounds', 'value': 'trb'},
                # Add more options for other statistics
            ],
            placeholder="Select a statistic"
        ),
        
        # Slider to filter by player position
        dcc.Slider(
            id='position-slider',
            min=0,
            max=4,
            step=1,
            marks={
                0: 'PG',
                1: 'SG',
                2: 'SF',
                3: 'PF',
                4: 'C'
            },
            value=0
        ),
        
        # Bar plot for team statistics
        dcc.Graph(id='team-barplot')
    ])

# Callback to update the team bar plot based on selected stat and position
@app.callback(
    Output('team-barplot', 'figure'),
    [Input('stat-dropdown', 'value'),
     Input('position-slider', 'value')]
)
def update_team_barplot(stat, position):
    # If a statistic is selected
    if stat:
        # Map position slider value to position
        pos_mapping = {0: 'PG', 1: 'SG', 2: 'SF', 3: 'PF', 4: 'C'}
        filtered_df = df[df['pos'] == pos_mapping[position]]
        
        # Aggregate team stats by summing up the selected stat
        team_stats = filtered_df.groupby('bref_team_id').sum()[stat]
        top_5_teams = team_stats.nlargest(5)
        
        # Create a bar plot
        fig = px.bar(top_5_teams, x=top_5_teams.index, y=stat, title=f"Top 5 Teams in {stat}")
        return fig
    # Return an empty plot if no stat is selected
    return {}

# Run the application
if __name__ == '__main__':
    app.run_server(debug=True)
