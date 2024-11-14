CREATE DATABASE IF NOT EXISTS spotify_db;


// Secure connection between snowflake and s3
create or replace storage integration s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::248189920361:role/spotify-spark-snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-bansari')
    COMMENT = 'Creating connection to S3'


describe integration s3_init;

// create file format object
CREATE OR REPLACE FILE FORMAT csv_fileformat
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE;


// stage to access s3  
CREATE OR REPLACE STAGE spotify_stage
    URL = 's3://spotify-etl-project-bansari/transformed-data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_fileformat;

    
LIST @spotify_stage;

CREATE OR REPLACE TABLE tbl_album(
    album_id STRING,
    album_name STRING,
    release_date DATE,
    total_tracks INT,
    url STRING
    
);

CREATE OR REPLACE TABLE tbl_artist(
    artist_id STRING,
    artist_name STRING,
    external_url STRING
    
);

CREATE OR REPLACE TABLE tbl_songs(
    song_id STRING,
    track_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added DATE,
    album_id STRING,
    artist_id STRING
);


CREATE OR REPLACE SCHEMA pipe;

CREATE OR REPLACE pipe spotify_db.pipe.tbl_album_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_album
FROM @spotify_db.public.spotify_stage/
pattern = 'album/.*';


CREATE OR REPLACE pipe spotify_db.pipe.tbl_artist_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_artist
FROM @spotify_db.public.spotify_stage/
pattern = 'artist/.*';


CREATE OR REPLACE pipe spotify_db.pipe.tbl_songs_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_songs
FROM @spotify_db.public.spotify_stage/
pattern = 'songs/.*';

COPY INTO TBL_ALBUM
FROM @spotify_stage/album/transformed_album_2024-11-12/run-1731412856125-part-r-00000;

COPY INTO TBL_ARTIST
FROM @spotify_stage/artist/transformed_album_2024-11-12/run-1731412862508-part-r-00000;

COPY INTO TBL_SONGS
FROM @spotify_stage/songs/transformed_album_2024-11-12/run-1731412863862-part-r-00000
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\n')
ON_ERROR = 'CONTINUE';

SELECT count(*) FROM tbl_album;

SELECT count(*) FROM tbl_artist;

SELECT count(*) FROM tbl_songs;

DESC pipe pipe.tbl_album_pipe;

DESC pipe pipe.tbl_artist_pipe;

DESC pipe pipe.tbl_songs_pipe;

DELETE FROM TBL_ALBUM;

SELECT SYSTEM$PIPE_STATUS('pipe.tbl_songs_pipe');



    