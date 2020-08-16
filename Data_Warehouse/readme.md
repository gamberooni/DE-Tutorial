# Project: Data Warehouse
## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to build an ETL Pipeline that extracts their data from S3, stage it in Redshift and then transform the staging data into a set of fact and dimension tables for analytics purposes such as discovering insights to what songs their users are listening to.

## Project Description
The project will use Amazon Redshift as a data warehouse and S3 as data storage. An ETL pipeline is built for a database hosted on Redshift. Data is loaded from S3 to staging tables on Redshift and execute SQL Statements that create a star schema (fact and dimension tables) from these staging tables. The AWS Python SDK known as boto3 is used in this project. 

## Data
1. Song filepath = s3://udacity-dend/song_data 
2. Log filepath = s3://udacity-dend/log_data 
3. Log Data filepath = s3://udacity-dend/log_json_path.json

## Redshift Configuration File
The Redshift configuration parameters are stored in the "dwh.cfg" file. You will have to generate your "key" and "secret" from your AWS account. Input your cluster endpoint into "HOST" and your IAM role ARN into "ARN" after creating your Redshift cluster and IAM role respectively. 

## Project Workflow
A star schema (fact and dimension tables) and two staging tables (song files and log files staging tables) will be created in Redshift using the PostgreSQL engine. The relevant data from the song files, log files and log data files will be extracted from the specified S3 buckets and loaded into the staging tables. From the staging tables, relevant fields will be selected and then inserted into the fact and dimension tables. 
