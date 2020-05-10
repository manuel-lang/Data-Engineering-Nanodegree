# Data Warehouse

Project Data Warehouse as part of the Udactiy Data Engineer Nanodegree.

## Project Summary
An implementation of a Data Warehouse leveraging AWS RedShift. This projects contains the ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team.

The data on S3 contains song and log information from a music store. This solution enables music stores to easily process loads of information efficiently.

## Purpose of this project
This projects processes data from different sources (in this case multiple S3 buckets) in a way that it can be analyzed easily and efficiently. The startup Sparkify thus enjoys the eased analysis of the run-time data of their application.

## Project instructions
1. Setup a redshift cluster on AWS and insert the connection details in `dwh.cfg`.
2. Create the needed the database structure by executing `create_tables.py`.
3. Process the data from the configured S3 data sources by executing `etl.py`.

## Database schema
| Table | Description |
| ---- | ---- |
| staging_events | stating table for event data |
| staging_songs | staging table for song data |
| songplays | information how songs were played, e.g. when by which user in which session | 
| users | user-related information such as name, gender and level | 
| songs | song-related information containing name, artist, year and duration | 
| artists | artist name and location (geo-coords and textual location) | 
| time | time-related info for timestamps | 

## ETL pipeline
1. Load song and log data both from S3 buckets.
2. Stage the loaded data.
3. Transform the data into the above described data schema.

## Example queries

* Find all users at a certain location: ```SELECT DISTINCT users.user_id FROM users JOIN songplays ON songplays.user_id = users.user_id WHERE songplays.location = <LOCATION>```
* Find all songs by a given artist: ```SELECT songs.song_id FROM songs JOIN artists ON songs.artist_id = artists.artist_id WHERE artist.name = <ARTIST>```
