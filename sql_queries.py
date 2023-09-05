import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS \"user\";"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession VARCHAR,
    lastName VARCHAR,
    length DOUBLE PRECISION,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts VARCHAR,
    userAgent VARCHAR,
    userId INT);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id VARCHAR NOT NULL,
    artist_latitude DECIMAL(9),
    artist_longitude DECIMAL(9),
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR NOT NULL,
    duration DECIMAL(9),
    num_songs INTEGER,
    title VARCHAR,
    year INTEGER
    );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(
    songplay_id INT IDENTITY(0,1) NOT NULL PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INT,
    level VARCHAR(20),
    song_id VARCHAR(50) NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INT NOT NULL,
    location VARCHAR,
    user_agent VARCHAR);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS "user"(
    user_id INT PRIMARY KEY,
    first_name VARCHAR, 
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song(
    song_id VARCHAR NOT NULL PRIMARY KEY,
    title VARCHAR(200),
    artist_id VARCHAR NOT NULL, 
    year INT, 
    duration DOUBLE PRECISION);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist(
    artist_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL, 
    location VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time BIGINT PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL);
""")

# STAGING TABLES
songData = config.get('S3','SONG_DATA')
logData = config.get('S3','LOG_DATA')
logJsonPath = config.get('S3','LOG_JSONPATH')
arn = config.get('IAM_ROLE','ARN')

staging_events_copy = ("""copy staging_events 
from {} 
credentials 'aws_iam_role={}'
JSON {};
""").format(logData,arn,logJsonPath)

staging_songs_copy = ("""copy staging_songs from {}
credentials 'aws_iam_role={}' json 'auto';
""").format(songData,arn)




# FINAL TABLES

songplay_table_insert = ("""INSERT INTO 
    songplay(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time, userId , level, song_id , artist_id , sessionId, location , userAgent
    FROM staging_events se
    JOIN staging_songs ss ON se.song=ss.title AND se.artist=ss.artist_name
    WHERE se.page='NextSong';        
""")

user_table_insert = ("""INSERT INTO "user"(user_id,first_name,last_name,gender,level)
    SELECT DISTINCT userId,firstName,lastName,gender,level
    FROM staging_events
    WHERE userId IS NOT NULL
    ORDER BY userId DESC;
""")

song_table_insert = ("""INSERT INTO song(song_id,title,artist_id,year,duration)
    SELECT DISTINCT song_id,title,artist_id,year,duration
    FROM staging_songs
    ORDER BY song_id DESC; 
""")

artist_table_insert = ("""INSERT INTO artist(artist_id,name,location,latitude,longitude)
    SELECT DISTINCT artist_id,artist_name,artist_location,artist_latitude,artist_longitude
    FROM staging_songs
    ORDER BY artist_id DESC;    
""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT 
    CAST(EXTRACT(epoch FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS BIGINT) as start_time,
    EXTRACT(hour FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS hour,
    EXTRACT(day FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS day,
    EXTRACT(week FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS week,
    EXTRACT(month FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS month,
    EXTRACT(year FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS year,
    EXTRACT(dayofweek FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS weekday
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
