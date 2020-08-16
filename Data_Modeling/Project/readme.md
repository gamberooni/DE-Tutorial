# Project: Data Modeling with Postgres
## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. A database schema and ETL pipeline are to be created for this optimized queries and analysis. 

## Project Description
In this project, data modeling and an ETL pipeline is done using PostgreSQL and Python. A database utilizing the star schema containing fact and dimenstion tables is created. The ETL pipeline transfers data from files in two local directories (song files and log files) into these tables in Postgres using Python and SQL. 

## Data
The data is extracted from JSON files residing in the "data" directory. These are just a part of the Million Song Dataset.

The song files consist of relevant fields for the songs table and artists table. The log files consist of relevant fields for the time table and users table. The fields in the songplays table takes in certain fields from all the tables -- songs, artists, time, users. This is achieved by using JOIN statements. 

## Project Workflow
First, a database called "sparkifydb" is created, along with tables -- artists, songs, time, songplays, users. All the SQL queries resides in "sql_queries.py". The file "etl.py" extracts the relevant data and load them into each table.  
