import json
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # TODO implement
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    client_credential_manager = spotipy.SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credential_manager)
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"
    playlist_uri = playlist_link.split("/")[-1]
    data = sp.playlist_tracks(playlist_uri)
    client = boto3.client('s3')
    file_name = "spotify_raw_" + str(datetime.now()) + ".json"
    client.put_object(
        Bucket = "spotify-etl-project-bansari",
        Key = "raw-data/to-processed/" + file_name,
        Body = json.dumps(data)
    )

    glue = boto3.client("glue")
    gluejobname = "spark_spotify_transformation_aws"

    try:
        runId = glue.start_job_run(JobName = gluejobname)
        status = glue.get_job_run(JobName = gluejobname, RunID = runId['JobRunId'])
        print("Job Status: ", status['JobRun']['JobRunState'])
    except Exception as e:
        print(e)