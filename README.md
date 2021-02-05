# Song data analysis

## Summary of the project
Data sources:
1. Song json files containing song and artist information
2. Log file containing user,time and songplay session info

In this project, we're running our pipeline for the above mentioned data files.
 
Steps:
 1. We're populating Songs and artist table from songs data.  
 2. Use these two tables to replace song name and artist names in log file while populating values to 
    songplay table. 
 3. Extract user information from these log files. We derive a separate time table for future 
    analysis based on the timestamp data from logs

## Schema
```
T_SONG => song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration float
T_ARTIST => artist_id varchar PRIMARY KEY, artist_name varchar, location varchar, latitude float, longitude float
T_TIME_TABLE => start_time timestamp, day int, week int, month int, year int, weekday int
T_USER => user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar
T_SONG_PLAY => songplay_id SERIAL PRIMARY KEY,start_time timestamp, user_id int, level varchar,song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar
```
## Relational mappings:
artist_id : 
primary key in artist table, acts as a foreign key in song play table

song_id : 
primary key in song table, acts as a foreign key in song play table

time :  
this is more of a derived table for storing dimension for future analysis.
 We're not using this table in project as of now
 
 # Constraints:
 Since there was multiple null values for most of the columns, FAQ suggested not to use too many constraints.
 We've added Primary key constraints for song and artist to avoid duplicate entried in database

## Installation
Steps to follow:
1. Run create_tables.py file to generate tables with proper schema.
2. Once tables are created, run etl.py file to populate tables from text files

```bash
python create_tables.py
python etl.py
```

## Future scope of this project
We can analyse the table to find out which artists and songs are trending, the user patterns for listening songs

## N.B.
Since there were many missing values, we've cleaned up data in case primary key 
was missing for some of the tables. 

If we have more common data points for relations, 
we can run certain join and aggregation queries to draw inferences from data.