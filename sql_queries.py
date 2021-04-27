import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')

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
    CREATE TABLE staging_events (
        artist VARCHAR(500), 
        auth VARCHAR(100), 
        firstName VARCHAR(500), 
        gender VARCHAR(50), 
        itemInSession INTEGER, 
        lastName VARCHAR(500), 
        length REAL, 
        level VARCHAR(50), 
        location VARCHAR(500), 
        method VARCHAR(50), 
        page VARCHAR(100), 
        registration REAL, 
        sessionId INTEGER, 
        song VARCHAR(500), 
        status INTEGER, 
        ts BIGINT, 
        userAgent VARCHAR(300), 
        userId INTEGER
    )
    DISTSTYLE AUTO;
""")


staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        song_id VARCHAR(20),
        num_songs INTEGER,
        title VARCHAR(500),
        artist_name VARCHAR(500),
        artist_latitude REAL,
        year INTEGER,
        duration REAL,
        artist_id VARCHAR(20), 
        artist_longitude REAL, 
        artist_location VARCHAR(500)
    )
    DISTSTYLE AUTO;
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
        start_time TIMESTAMP, 
        user_id INTEGER, 
        level VARCHAR(10), 
        song_id VARCHAR(20) NOT NULL, 
        artist_id VARCHAR(20) NOT NULL, 
        session_id INTEGER NOT NULL, 
        location VARCHAR(500), 
        user_agent VARCHAR(150)
    )
    DISTSTYLE AUTO;
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id INTEGER PRIMARY KEY, 
        first_name VARCHAR(500), 
        last_name VARCHAR(500), 
        gender VARCHAR(50), 
        level VARCHAR(50)
    )
    DISTSTYLE AUTO;
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR(20) PRIMARY KEY, 
        title VARCHAR(500), 
        artist_id VARCHAR(20) NOT NULL, 
        year INTEGER, 
        duration REAL
    )
    DISTSTYLE AUTO;
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR(20) PRIMARY KEY, 
        name VARCHAR(500), 
        location VARCHAR(500), 
        latitude REAL, 
        longitude REAL
    )
    DISTSTYLE AUTO;
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY, 
        hour INTEGER, 
        day INTEGER, 
        week INTEGER, 
        month INTEGER, 
        year INTEGER, 
        weekday INTEGER
    )
    DISTSTYLE AUTO;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS json {}
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)


staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS json 'auto'
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES
        
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, 
        session_id, location, user_agent)
    SELECT timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second' AS start_time,
                    e.userId, 
                    e.level, 
                    s.song_id, 
                    s.artist_id, 
                    e.sessionId, 
                    e.location, 
                    e.userAgent
    FROM staging_events e
    JOIN staging_songs s
    ON LOWER(TRIM(e.artist)) = LOWER(TRIM(s.artist_name))
    AND LOWER(TRIM(e.song)) = LOWER(TRIM(s.title))
    AND e.length=s.duration
    AND e.page='NextSong'
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE userId is not null
""")
    

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id is not null
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id is not null
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT DISTINCT start_time, 
        EXTRACT(hour FROM start_time) as hour, 
        EXTRACT(day FROM start_time) as day, 
        EXTRACT(week FROM start_time) as week, 
        EXTRACT(month FROM start_time) as month, 
        EXTRACT(year FROM start_time) as year, 
        EXTRACT(weekday FROM start_time) as weekday
    FROM  songplays
""")

# QUERY LISTS

create_table_queries = [staging_songs_table_create, staging_events_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
 