# Analysing user song plays with Redshift

A startup called Sparkify has been collecting data on songs and user activity on their new music streaming app. Their data resides on Amazon S3 and they want to analyze the data.
The data consists of log information on user behavior on the app and information on songs on their app.

We here create an ETL pipeline that extracts data from the S3 Buckets, loads it into staging and transforms it and inserts it into specified fact and dimensional tables in Redshift. 
This document details how to run the scripts and provides explanation of the files in the repository. 

## Schema
The schema for this database follows star model consisting of one fact table, songplays, and four dimension tables users, artists, time and songs.

## How it works

sql_queries.py contains the queries for creating and dropping tables, staging data and for inserting data into the database for all our tables. 

For running first create tables by running create_tables.py. This creates tables based on queries in sql_queries.py. Then run etl.py that using queries on sql_queries.py first loads data into staging and then into the created tables in the database.

## Datasets

The songs data contains metadata of songs in JSON format. Here is an example of the songs data:

 {
   "num_songs": 1, 
   "artist_id": 
   "ARJIE2Y1187B994AB7", 
   "artist_latitude": null, 
   "artist_longitude": null, 
   "artist_location": "", 
   "artist_name": "Line Renaud", 
   "song_id": "SOUPIRU12A6D4FA1E1", 
   "title": "Der Kleine Dompfaff", 
   "duration": 152.92036, 
   "year": 0
  }

The log files contain information on what and how the end user was listening to in JSON. Here is an example of a log file
  
  {
  "artist":null,
  "auth":"Logged In",
  "firstName":"Walter",
  "gender":"M",
  "itemInSession":0,
  "lastName":"Frye",
  "length":null,
  "level":"free",
  "location":"San Francisco-Oakland-Hayward, CA",
  "method":"GET",
  "page":"Home",
  "registration":1540919166796.0,
  "sessionId":38,
  "song":null,
  "status":200,
  "ts":1541105830796,
  "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143     Safari\/537.36\"",
  "userId":"39"
  }

## Description of files in repository

**sql_queries.py** 
- This file contains queries for dropping and creation of database tables.
- Also contains queries for for inserting data into staging and for final tables. 
- The queries in this file are inputs for etl.py and create_tables.py

**create_tables.py**
- This file creates and drops databases.
- This file should be run before running etl.py

**etl.py**
- This file contains the ETL process for extracting data, loading it into staging and finally to the created Redship tables.

**dwh.cfg**
- This file contains the configuration information used by sql_quesries.
