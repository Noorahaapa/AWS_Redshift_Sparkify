import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
 CREATE TABLE IF NOT EXISTS staging_events (
     artist varchar,
     auth varchar,
     firstName varchar,
     gender varchar,
     itemInSession int,
     lastName varchar,
     length real,
     level varchar,
     location varchar,
     method varchar,
     page varchar,
     registration decimal,
     sessionId int,
     song varchar,
     status int,
     ts bigint,
     userAgent varchar,
     userId varchar);
""")

staging_songs_table_create = ("""
 CREATE TABLE IF NOT EXISTS staging_songs (
     num_songs int,
     artist_id varchar,
     artist_latitude float,
     artist_longitude float,
     artist_location varchar,
     artist_name varchar,
     song_id varchar,
     title varchar,
     duration float,
     year int);
""")

user_table_create = ("""
  CREATE TABLE IF NOT EXISTS users (
    user_id varchar NOT NULL,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar NOT NULL,
    PRIMARY KEY (user_id)); 
""")

song_table_create = ("""
  CREATE TABLE IF NOT EXISTS songs (
    song_id varchar NOT NULL,
    title varchar,
    artist_id varchar,
    year int,
    duration float,
    PRIMARY KEY (song_id)); 
""")

artist_table_create = ("""
  CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar NOT NULL,
    name varchar,
    location varchar,
    latitude float,
    longitude float,
    PRIMARY KEY (artist_id)); 
""")

time_table_create = ("""
  CREATE TABLE IF NOT EXISTS time (
    start_time timestamp NOT NULL,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int,
    PRIMARY KEY (start_time)); 
""")

songplay_table_create = ("""
  CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0,1),
    start_time timestamp NOT NULL,
    user_id varchar NOT NULL,
    level varchar NOT NULL,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar,
    PRIMARY KEY (songplay_id));  
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    FORMAT AS JSON {}
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    FORMAT AS JSON 'auto'
""").format(config.get('S3','SONG_DATA'), 
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

user_table_insert = ("""
  INSERT INTO users(user_id, first_name, last_name, gender, level)
      SELECT DISTINCT userId as user_id,
                      firstName as first_name,
                      lastName as last_name,
                      gender,
                      level
      FROM staging_events
      WHERE page='NextSong' AND
            user_id IS NOT NULL AND
            user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
  INSERT INTO songs(song_id, title, artist_id, year, duration)
      SELECT DISTINCT song_id,
                      title,
                      artist_id,
                      year,
                      duration
      FROM staging_songs
      WHERE song_id IS NOT NULL
""")


artist_table_insert = ("""
  INSERT INTO artists (artist_id, name, location, latitude, longitude)
      SELECT DISTINCT artist_id,
                      artist_name as name,
                      artist_location as location,
                      artist_latitude as latitude,
                      artist_longitude as longitude
      FROM staging_songs
      WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists) AND
            artist_id IS NOT NULL
""")

#Timestamp conversion created with help of the discussion
#https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift

songplay_table_insert = ("""
  INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
      SELECT DISTINCT timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second' as start_time, 
                      u.user_id,
                      u.level,
                      s.song_id,
                      a.artist_id,
                      se.sessionId as session_id,
                      a.location,
                      se.userAgent as user_agent
      FROM staging_events se
      JOIN users u ON (se.userId=u.user_id)
      JOIN artists a ON (se.artist=a.name)
      JOIN songs s ON (a.artist_id = s.artist_id)
      WHERE se.page = 'Next Song' 
""")

#Timestamp conversion creating with help of the discussion
#https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift

time_table_insert = ("""
  INSERT INTO time (start_time, hour, day, week, month, year, weekday)
      SELECT DISTINCT timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second' as start_time,
    extract(HOUR FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as hour,
    extract(DAY FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as day,
    extract(WEEK FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as week,
    extract(MONTH FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as month,
    extract(YEAR FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as year,
    extract(DAY FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as weekday
      FROM staging_events se
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
