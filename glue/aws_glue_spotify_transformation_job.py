
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, col, to_date
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
import boto3
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_path = "s3://spotify-etl-project-bansari/raw-data/to-processed/"

source_df = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": [s3_path]}
)

spotify_df = source_df.toDF()

def process_albums(df):
    album_df = df.withColumn("items", explode("items")).select(
        col("items.track.album.id").alias("album_id"),
        col("items.track.album.name").alias("album_name"),
        col("items.track.album.release_date").alias("release_date"),
        col("items.track.album.total_tracks").alias("total_tracks"),
        col("items.track.album.external_urls.spotify").alias("url")
    ).drop_duplicates(["album_id"])
    return album_df


def process_artists(df):
    exploded_items_df = df.select(explode("items").alias("item"))
    artists_df_exploaded = exploded_items_df.select(explode("item.track.artists").alias("artist"))
    artist_df = artists_df_exploaded.select(
        col("artist.id").alias("artist_id"),
        col("artist.name").alias("artist_name"),
        col("artist.external_urls.spotify").alias("external_url")
    ).drop_duplicates(["artist_id"])
    return artist_df


def process_songs(df):
    exploaded_df = df.select(explode("items").alias("item"))
    songs_df = exploaded_df.select(
        col("item.track.id").alias("song_id"),
        col("item.track.name").alias("track_name"),
        col("item.track.duration_ms").alias("duration_ms"),
        col("item.track.external_urls.spotify").alias("url"),
        col("item.track.popularity").alias("popularity"),
        col("item.added_at").alias("song_added"),
        col("item.track.album.id").alias("album_id"),
        col("item.track.artists")[0]["id"].alias("artist_id")
    ).drop_duplicates(["song_id"])
    songs_df = songs_df.withColumn("song_added", to_date(col("song_added")))
    return songs_df


df_album = process_albums(spotify_df)
df_artist = process_artists(spotify_df)
df_song = process_songs(spotify_df)


def write_to_s3(df, path_suffix, format_type="csv"):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": f"s3://spotify-etl-project-bansari/transformed-data/{path_suffix}"},
        format=format_type
    )

write_to_s3(df_album, "album/transformed_album_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
write_to_s3(df_artist, "artist/transformed_album_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
write_to_s3(df_song, "songs/transformed_album_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")

def list_s3_objects(bucket, prefix):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.json')]
    return keys


bucket_name = "spotify-etl-project-bansari"
prefix = "raw-data/to-processed/"
spotify_keys = list_s3_objects(bucket_name, prefix)


def move_and_delete(spotify_keys, Bucket):
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        destination_key = 'raw-data/processed/' + key.split('/')[-1]
        s3_resource.meta.client.copy(copy_source, bucket_name, destination_key)
        s3_resource.Object(Bucket, key).delete()

move_and_delete(spotify_keys, bucket_name)
job.commit()