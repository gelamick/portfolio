NBA Player & Team Comparison Dashboard

Description

This Dash application allows users to compare the performance of NBA players and teams using a CSV file containing statistics. Two main features are provided:

    Player Comparison: Select "rookies" (players under 24 years old) and "seniors" (players 24 years or older) to compare their individual statistics.
    Team Comparison: Select a statistic (assists, points, rebounds, etc.) to see which teams excel in that area, with a ranking of the top 5 teams.
    You can also filter teams based on player positions (point guard, forward, center, etc.).

Included Files

    app.py: The main script that contains the Dash code to run the application.
    requirements.txt: Dependencies required to run the application.
    nba_2013.csv: The CSV file containing player and team data (to be added if you have the data).

Prerequisites  : Python 3.7 or later

Installation

    Navigate to the project directory
    *Create environment**
    python3 -m venv mon_env
    source mon_env/bin/activate (Linux)
    Install the required dependencies (pip install -r requirements.txt)
    
    Run and access the application (python app.py)

Visual

    Open your browser and go to: http://127.0.0.1:8050/