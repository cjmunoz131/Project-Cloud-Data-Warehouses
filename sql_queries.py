import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')
IAM_ROLE = config.get('IAM_ROLE','ARN')
REGION = 'us-west-2'

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# staging tables
staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist varchar(255)  NULL,
        auth varchar(10)  NULL,
        firstName varchar(255)  NULL,
        gender varchar(10)  NULL,
        itemInSession int  NULL,
        lastName varchar(255)  NULL,
        length decimal(9,5)  NULL,
        level varchar(10)  NULL,
        location varchar(255)  NULL,
        method varchar(10)  NULL,
        page varchar(50)  NULL,
        registration varchar(500)  NULL,
        sessionId int  NOT NULL,
        song varchar(500)  NULL,
        status int  NULL,
        ts bigint  NOT NULL,
        userAgent varchar(500)  NULL,
        userId int  NULL,
        id bigint identity(0,1)  NOT NULL,
        CONSTRAINT staging_events_pk PRIMARY KEY (id)
    )  DISTSTYLE EVEN;
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_song int  NULL,
        artist_id varchar(100)  NOT NULL,
        artist_latitude decimal(11,8)  NULL,
        artist_longitude decimal(11,8)  NULL,
        artist_location varchar(255)  NULL,
        artist_name varchar(255)  NULL,
        song_id varchar(50)  NOT NULL,
        title varchar(500)  NULL,
        duration decimal(9,5)  NULL,
        year int  NULL,
        CONSTRAINT staging_songs_pk PRIMARY KEY (song_id)
    )  DISTSTYLE EVEN;
""")

#Analytics Table
songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id int identity(0,1)  NOT NULL,
        start_time timestamp  NOT NULL,
        level varchar(10)  NOT NULL,
        session_id varchar(50)  NOT NULL,
        location varchar(255)  NULL,
        user_agent varchar(500)  NULL,
        user_id int  NOT NULL,
        artist_id varchar(100)  NOT NULL,
        song_id varchar(50)  NOT NULL,
        CONSTRAINT songplay_id PRIMARY KEY (songplay_id)
    )  DISTSTYLE KEY DISTKEY (user_id) COMPOUND SORTKEY (user_id, start_time, song_id);
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id int  NOT NULL,
        first_name varchar(255)  NULL,
        last_name varchar(255)  NULL,
        gender varchar(10)  NULL,
        level varchar(10)  NULL,
        CONSTRAINT id PRIMARY KEY (user_id)
    )  DISTSTYLE ALL SORTKEY (user_id);
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id varchar(50)  NOT NULL,
        title varchar(500)  NOT NULL,
        artist_id varchar(100)  NOT NULL,
        year smallint  NULL,
        duration decimal(9,5)  NULL,
        CONSTRAINT songs_pk PRIMARY KEY (song_id)
    )  DISTSTYLE ALL SORTKEY (song_id);
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id varchar(100)  NOT NULL,
        name varchar(500)  NULL,
        location varchar(500)  NULL,
        latitude decimal(11,8)  NULL,
        longitude decimal(11,8)  NULL,
        CONSTRAINT artists_pk PRIMARY KEY (artist_id)
    )  DISTSTYLE ALL SORTKEY (artist_id);
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time timestamp  NOT NULL,
        hour smallint  NULL,
        day smallint  NULL,
        week smallint  NULL,
        month smallint  NULL,
        year smallint  NULL,
        weekday smallint  NULL,
        CONSTRAINT time_pk PRIMARY KEY (start_time)
    )  DISTSTYLE ALL SORTKEY (start_time);
""")

# COPY COMMAND FOR STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {}
    iam_role {}
    format as json {}
    STATUPDATE ON
    EMPTYASNULL
    BLANKSASNULL
    region '{}';
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH, REGION)

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    format as json 'auto'
    STATUPDATE ON
    EMPTYASNULL
    BLANKSASNULL
    ACCEPTINVCHARS AS '^'
    region '{}';
""").format(SONG_DATA, IAM_ROLE, REGION)

# INSERTs WITH TRANSFORMATIONS FOR FINAL TABLES

song_table_insert = ("""
    insert into songs (song_id,
        title,
        artist_id,
        year,
        duration)
    select distinct song_id,
        title,
        artist_id,
        year,
        duration
    from staging_songs;
""")

user_table_insert = ("""
    insert into users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    select distinct userid,
        firstName as firstname,
        lastName as lastname,
        gender as gender,
        level as level
    from staging_events where page = 'NextSong'
""")

songplay_table_insert = ("""
    insert into songplays (start_time,
        level,
        session_id,
        location,
        user_agent,
        user_id,
        artist_id, 
        song_id)
    select DISTINCT timestamp 'epoch' + evs.ts/1000 * interval '1 second' as start_time,
        evs.level AS level,
        evs.sessionid AS session_id,
        evs.location AS location,
        evs.useragent AS user_agent,
        evs.userid AS user_id,
        ss.artist_id AS artist_id,
        ss.song_id AS song_id
    from staging_events as evs, staging_songs as ss where evs.song = ss.title and evs.page = 'NextSong'
""")

#and evs.artist = ss.artist_name    evs.song = ss.title
artist_table_insert = ("""
    insert into artists (
        artist_id,
        name,
        location,
        latitude,
        longitude)
    select distinct artist_id as artist_id, 
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    from staging_songs;
""")

time_table_insert = ("""
    insert into time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    select distinct timestamp 'epoch' + ts/1000 * interval '1 second' as start_time,
        EXTRACT(hour from  start_time) as hour,
        EXTRACT(day from  start_time) as day,
        EXTRACT(week from  start_time) as week,
        EXTRACT(month from  start_time) as month,
        EXTRACT(year from  start_time) as year,
        EXTRACT(dayofweek from  start_time) as weekday
    from staging_events  where  page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
