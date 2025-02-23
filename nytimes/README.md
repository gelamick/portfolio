
## NY Times API Project

This project was carried out as part of the Data Engineer training at Datascientest (Data Engineer cohort, January 2024). 

# Overview

The goal of this project is  (via web scraping). to leverage the New York Times Developer Portal, which provides several APIs, to create a custom API for gathering and processing data from the NY Times. 
This project collects data on articles, books, and additional pricing information
We also developed a dashboard using Dash to visualize the data, and the entire application is containerized with several Docker files for easy deployment.
In addition, we implemented a Machine Learning model using a Support Vector Machine (SVM) and a Linear Regression Model to predict the section (category) of a news article based on its title and introductory paragraph. 
The project also includes a FastAPI endpoint to allow predictions for new articles.

# Project Breakdown

1. Data Collection

We collected data using the following APIs from the NY Times Developer Portal:

    Article Search API: Retrieves articles from the New York Times, allowing users to search for articles by keyword, date, and other criteria.
    Books API: Fetches metadata about bestsellers, including details about the books, their authors, and genres.

Additionally, we scraped websites to gather pricing information related to the books listed on the NY Times bestseller list. This allowed us to provide comprehensive data on where users can purchase these books.

Tools & Techniques:

    Requests library: For making API calls to gather data.
    BeautifulSoup: For web scraping to collect book price data.

MongoDB Collections:

We used MongoDB to store the data in three collections:

    Books: Information on NY Times bestsellers, including title, author, and genre.
    Articles: Metadata and content from NY Times articles.
    Prices: Pricing data for books collected via web scraping from various online sources.

Deliverables:

    A file (PDF or DOC) documenting the APIs used, data collected, and the data handling process.
    Sample datasets for books, articles, and prices in JSON format. (200 000)

2. Data Architecture

The data architecture was designed to handle different types of data efficiently. MongoDB was used to store the following collections:

    Books Collection: Stores metadata for NY Times bestsellers, including title, author, and purchase links.
    Articles Collection: Stores NY Times articles retrieved via the Article Search and Times Wire APIs.
    Prices Collection: Contains book pricing information scraped from various online bookstores.

Using MongoDB allowed for flexible storage and easy querying of the diverse data types.

Tools & Techniques:

    MongoDB: For managing the data, creating collections, and running queries.

Deliverable:

    Three MongoDB collections (Books, Articles, Prices) with data accessible via API calls.

![alt tag]( https://github.com/DataScientest-Studio/gelamick/architecture.png)

3. Data Consumption & Dashboard

To make the data accessible and easy to understand, we created a visual dashboard using Dash. The dashboard allows users to explore:

    Books Collection: View bestseller metadata, filter by genre, and see pricing information.
    Articles Collection: Browse NY Times articles by keyword or date, and view detailed article metadata.
    Prices Collection: See the best purchase options for books based on price.

The dashboard is interactive, allowing users to filter and explore data visually.
Tools & Techniques:

    Dash: For building the interactive dashboard and visualizations.
    Plotly: For creating the visual graphs and charts displayed on the dashboard.

Deliverable:

    A fully functional dashboard with multiple views for books, articles, and pricing data.

![alt tag]( https://github.com/DataScientest-Studio/gelamick/visuel_1.png)


4. Machine Learning Model - Support Vector Machine (SVM)

A Support Vector Machine (SVM) model was created to predict the section (category) of a news article based on its title and introduction. 
The model is trained on NY Times article data and predicts sections such as "Sports", "Politics", "Technology", etc.
Features:

    Training Dataset: Articles with known sections, titles, and introductory paragraphs.
    Prediction: The model predicts the section of a new article based on the provided title and intro.
    API Endpoint: A FastAPI endpoint allows users to send new articles in JSON format and receive a prediction for the section.

5. Production Deployment

To ensure the application can be deployed across different environments, we containerized it using Docker. The project includes six Docker files, which allow for the smooth running of all components, including:

    MongoDB: Container for the MongoDB database where the collections are stored.
    API Service: Container for the custom API that retrieves data from the MongoDB collections.
    Dash Application: Container for the dashboard displaying the data.
    Web Scraper: Container for running the web scraping process to gather book prices.
    API Gateway: Container for managing requests to the various services.

This setup ensures that all components are isolated and can be run independently or together.
Tools & Techniques:

    FastAPI: For developing the custom API that serves the data.
    Docker: For containerizing the entire application, including the API, MongoDB, Dash, and other services.
    Docker Compose: To orchestrate the services across multiple Docker containers.

Deliverable:

    A fully containerized application using six Docker files that can be deployed locally or on cloud platforms.

![alt tag]( https://github.com/DataScientest-Studio/gelamick/visuel_1.png)

5. Implementation 
git clone 
pip install -r requirements.txt
Configure API keys for the NY Times in a .env file.
Populate MongoDB with the articles, books data
Use Docker to set up and run the application: docker-compose up --build
Access the Dash dashboard at http://localhost:8050 and the FastAPI documentation at http://localhost:8000/docs.

-----------

**Team:**  

* Alfred Quitman ([LinkedIn](https://www.linkedin.com/in/))  
* Emmanuel Bompard ([LinkedIn](https://www.linkedin.com/in/))  
* Michael Abergel([LinkedIn](https://www.linkedin.com/in/michaelabergel/))  

**Supervised by :**  

* Rémy Dallavalle ([LinkedIn](https://www.linkedin.com/in/))  