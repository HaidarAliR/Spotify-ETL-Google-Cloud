from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import pandas as pd
import base64
import functions_framework
import json
import os
import pandas as pd
from playlists import spotify_playlists
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
import time
import os



def gather_data_local():
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret') 
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    spotipy_object = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
   
    # For every artist we're looking for
    final_data_dictionary = {
        'Spotify ID': [],
        'Name': [],
        'Artists': [],
        'Daily Rank': [],
        'Country ISO Code': [],
        'Snapshot Date': [],
        'Popularity': [],
        'Is Explicit': [],
        'Duration ms': [],
        'Album Name': [],
        'Album Release Date': [],
        'Danceability': [],
        'Energy': [],
        'Key': [],
        'Loudness': [],
        'Mode': [],
        'Speechiness': [],
        'Acousticness': [],
        'Instrumentalness': [],
        'Liveness': [],
        'Valence': [],
        'Tempo': [],
        'Time Signature': [],
        'Unique_id': []
    }
    playlists = spotify_playlists()
    for playlist_name in playlists:
        playlist = playlists[playlist_name]
        playlist_tracks = None
        while playlist_tracks is None:
            try:
                playlist_tracks = spotipy_object.playlist_tracks(playlist)
            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:
                    retry_after = int(e.headers['Retry-After'])
                    time.sleep(retry_after)
                else:
                    raise e

        track_uris = [item['track']['uri'] for item in playlist_tracks['items']]
        audio_features_list = None
        while audio_features_list is None:
            try:
                audio_features_list = spotipy_object.audio_features(track_uris)
            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:
                    retry_after = int(e.headers['Retry-After'])
                    time.sleep(retry_after)
                else:
                    raise e

        for i, (item, audio_features) in enumerate(zip(playlist_tracks['items'], audio_features_list)):
            track = item['track']
            album = track['album']

            try:
                audio_features = spotipy_object.audio_features(track['uri'])[0]
            except spotipy.exceptions.SpotifyException as e:
                print(f"Error getting audio features for track: {track['name']}")
                audio_features = {}  # Set audio_features as an empty dictionary
            


            final_data_dictionary['Spotify ID'].append(track['id'])
            final_data_dictionary['Name'].append(track['name'])
            final_data_dictionary['Artists'].append(', '.join([artist['name'] for artist in track['artists']]))
            final_data_dictionary['Daily Rank'].append(i+1)
            final_data_dictionary['Country ISO Code'].append(playlist_name)
            final_data_dictionary['Snapshot Date'].append(datetime.now().strftime("%Y-%m-%d"))
            final_data_dictionary['Popularity'].append(track['popularity'])
            final_data_dictionary['Is Explicit'].append(track['explicit'])
            final_data_dictionary['Duration ms'].append(track['duration_ms'])
            final_data_dictionary['Album Name'].append(album['name'])
            final_data_dictionary['Album Release Date'].append(album['release_date'])
            final_data_dictionary['Danceability'].append(audio_features.get('danceability'))  # Use .get() to handle missing keys
            final_data_dictionary['Energy'].append(audio_features.get('energy'))
            final_data_dictionary['Key'].append(audio_features.get('key'))
            final_data_dictionary['Loudness'].append(audio_features.get('loudness'))
            final_data_dictionary['Mode'].append(audio_features.get('mode'))
            final_data_dictionary['Speechiness'].append(audio_features.get('speechiness'))
            final_data_dictionary['Acousticness'].append(audio_features.get('acousticness'))
            final_data_dictionary['Instrumentalness'].append(audio_features.get('instrumentalness'))
            final_data_dictionary['Liveness'].append(audio_features.get('liveness'))
            final_data_dictionary['Valence'].append(audio_features.get('valence'))
            final_data_dictionary['Tempo'].append(audio_features.get('tempo'))
            final_data_dictionary['Time Signature'].append(audio_features.get('time_signature'))
            final_data_dictionary['Unique_id'].append(f"{track['id']}_{playlist_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}")

                
    df = pd.DataFrame(final_data_dictionary)
    df.rename(columns=lambda x: x.lower().replace(' ', '_'), inplace=True)

    return df


@functions_framework.cloud_event
def data_extract(cloud_event):
    #function starts here 
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret') 
    result=''
    current_time_seconds = time.time()
    local_time = time.localtime(current_time_seconds)

    formatted_local_time = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    timestamp = str(formatted_local_time).replace('-','_').replace(' ','_').replace(':','')
    # Declare your bucket name
    bucket_name = "spotify-top-50"      
    # Declare your project ID
    project_id = "managing-data-420007" 
    # Give the file a name along with the date and time 
    csv_filename="spotify_data_" + timestamp + ".csv"
    # Give a path to the to-processed directory
    destination_blob_name = f"raw-data/to-processed/{csv_filename}"
    # Give a path to the processed directory
    processed_blob_name = f"raw-data/processed/{csv_filename}"
    # Give a path to the failed directory
    failed_blob_name = f"raw-data/failed/{csv_filename}"

    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    spotify_object = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

    spotify_data = gather_data_local()

    # Loading data into json and saving it in to-processed folder 
    storage_client = storage.Client(project=project_id) # Initialize a client
    # Create a bucket object that accesses our bucket
    bucket = storage_client.bucket(bucket_name)
    # Create a blob object representing the json_filename
    blob = bucket.blob(destination_blob_name)
    # Convert the dataframe to a CSV string
    spotify_data_csv = spotify_data.to_csv(index=False)
    # Upload data to a storage blob directly from a string
    blob.upload_from_string(spotify_data_csv, content_type='text/csv')
    # 
    result = f"Data uploaded to gs://{bucket_name}/{destination_blob_name}"
    # print(result)

    # Declare variables for track_info table
    client = bigquery.Client(project_id)
    source_dataset_id = "spotify_top_50"
    source_table_id = "spotify_top_50_daily"
    source_table = f"{project_id}.{source_dataset_id}.{source_table_id}"

    # Construct a SQL query to check if the table exists
    source_query = f"""
    SELECT table_name
    FROM {project_id}.{source_dataset_id}.INFORMATION_SCHEMA.TABLES
    WHERE table_name = '{source_table_id}'
    """

    # Run the query
    source_query_job = client.query(source_query)

    # Check if the query returns any rows (i.e., if the table exists)
    exists = any(source_query_job)
    if exists:
        print(f"Table {source_table} exists.")
    else:
        sql=f"""
        CREATE OR REPLACE TABLE {source_table}
        (
            spotify_id STRING,
            name STRING,
            artists STRING,
            daily_rank INT64,
            country_iso_code STRING,
            snapshot_date DATE,
            popularity INT64,
            is_explicit BOOLEAN,
            duration_ms INT64,
            album_name STRING,
            album_release_date DATE,
            danceability FLOAT64,
            energy FLOAT64,
            key INT64,
            loudness FLOAT64,
            mode INT64,
            speechiness FLOAT64,
            acousticness FLOAT64,
            instrumentalness FLOAT64,
            liveness FLOAT64,
            valence FLOAT64,
            tempo FLOAT64,
            time_signature INT64,
            unique_id STRING
        )
        """
        source_query_job = client.query(sql)
        source_query_job.result()
        source_tab_creation=f'source table created: {source_table}'
        result=result+"\n"+source_tab_creation

    source_bucket = storage_client.get_bucket(bucket_name)
    prefix = 'raw-data/to-processed/'

      # Check if the folder is empty
    blobs = list(source_bucket.list_blobs(prefix=prefix)) 
    filename = f"gs://{bucket_name}/{destination_blob_name}"
    uri = filename
    if not blobs:
        print("The folder is empty.")
    else:
        # Loop through the blobs if you need to process multiple files
        for blob in blobs:
            # Construct the URI for the blob
            uri = filename
            try:
                # Define the job configuration for the BigQuery load job
                job_config = bigquery.LoadJobConfig(
                    autodetect=False, source_format=bigquery.SourceFormat.CSV,
                    max_bad_records=10  # Set the maximum number of bad records to 2
                )
                # Load the data from the blob into BigQuery
                load_job = client.load_table_from_uri(
                    uri, source_table, job_config=job_config
                )
                load_job.result()  # Wait for the job to complete.

                destination_table = client.get_table(source_table)
                source_load_data = f"Successfully loaded {csv_filename} into {destination_table}."
                print(source_load_data)

                # Move file to processed directory
                processed_blob_name = f"raw-data/processed/{csv_filename}"  # Modify as needed
                blob_new = source_bucket.copy_blob(blob, source_bucket, new_name=processed_blob_name)
                blob.delete()

            except GoogleCloudError as e:
                print(f"Failed to load file: {e}")

                # Move file to failed directory
                failed_blob_name = f"raw-data/failed/{csv_filename}"
                blob_fail = bucket.copy_blob(blob, bucket, new_name=failed_blob_name)
                blob.delete()
    moved_message=f"file moved to {processed_blob_name} directory"
    print(moved_message)
    result=result+"\n"+moved_message


    ##################################################################

    target_dataset_id = "spotify_top_50_datamart"
    target_table_id = "spotify_top_50_chart_daily"
    target_table = f"{project_id}.{target_dataset_id}.{target_table_id}"

    # Construct a SQL query to check if the table exists
    target_query = f"""
    SELECT table_name
    FROM {project_id}.{target_dataset_id}.INFORMATION_SCHEMA.TABLES
    WHERE table_name = '{target_table_id}'
    """
    target_query_job = client.query(target_query)

    # Check if the query returns any rows (i.e., if the table exists)
    exists = any(target_query_job)
    if exists:
        print(f"Table {target_table} exists.")
    else:
        sql=f"""
        CREATE TABLE {target_table}
        (
            spotify_id STRING,
            name STRING,
            artists STRING,
            daily_rank INT64,
            country_iso_code STRING,
            snapshot_date DATE,
            popularity INT64,
            is_explicit BOOLEAN,
            duration_ms INT64,
            album_name STRING,
            album_release_date DATE,
            danceability FLOAT64,
            energy FLOAT64,
            key INT64,
            loudness FLOAT64,
            mode INT64,
            speechiness FLOAT64,
            acousticness FLOAT64,
            instrumentalness FLOAT64,
            liveness FLOAT64,
            valence FLOAT64,
            tempo FLOAT64,
            time_signature INT64,
            unique_id STRING
        )
        """
        target_query_job = client.query(sql)
        target_query_job.result()
        target_tab_creation = f'target table crated: {target_table}'
        result = result+"\n"+target_tab_creation

    merge_query = f"""
    MERGE INTO `{target_table}` AS TARGET
    USING (
        SELECT
          spotify_id,
          name,
          artists,
          daily_rank,
          country_iso_code,
          snapshot_date,
          popularity,
          is_explicit,
          duration_ms,
          album_name,
          album_release_date,
          danceability,
          energy,
          key,
          loudness,
          mode,
          speechiness,
          acousticness,
          instrumentalness,
          liveness,
          valence,
          tempo,
          time_signature,
            unique_id
        FROM `{source_table}`
    ) SOURCE
    ON SOURCE.unique_id = TARGET.unique_id
    WHEN NOT MATCHED THEN 
        INSERT (
            spotify_id,
            name,
            artists,
            daily_rank,
            country_iso_code,
            snapshot_date,
            popularity,
            is_explicit,
            duration_ms,
            album_name,
            album_release_date,
            danceability,
            energy,
            key,
            loudness,
            mode,
            speechiness,
            acousticness,
            instrumentalness,
            liveness,
            valence,
            tempo,
            time_signature,
            unique_id
            ) 
        VALUES (
            SOURCE.spotify_id,
            SOURCE.name,
            SOURCE.artists,
            SOURCE.daily_rank,
            SOURCE.country_iso_code,
            SOURCE.snapshot_date,
            SOURCE.popularity,
            SOURCE.is_explicit,
            SOURCE.duration_ms,
            SOURCE.album_name,
            SOURCE.album_release_date,
            SOURCE.danceability,
            SOURCE.energy,
            SOURCE.key,
            SOURCE.loudness,
            SOURCE.mode,
            SOURCE.speechiness,
            SOURCE.acousticness,
            SOURCE.instrumentalness,
            SOURCE.liveness,
            SOURCE.valence,
            SOURCE.tempo,
            SOURCE.time_signature,
            SOURCE.unique_id
            )
    WHEN MATCHED THEN
        UPDATE SET
            TARGET.name = SOURCE.name,
            TARGET.artists = SOURCE.artists,
            TARGET.daily_rank = SOURCE.daily_rank,
            TARGET.country_iso_code = SOURCE.country_iso_code,
            TARGET.snapshot_date = SOURCE.snapshot_date,
            TARGET.popularity = SOURCE.popularity,
            TARGET.is_explicit = SOURCE.is_explicit,
            TARGET.duration_ms = SOURCE.duration_ms,
            TARGET.album_name = SOURCE.album_name,
            TARGET.album_release_date = SOURCE.album_release_date,
            TARGET.danceability = SOURCE.danceability,
            TARGET.energy = SOURCE.energy,
            TARGET.key = SOURCE.key,
            TARGET.loudness = SOURCE.loudness,
            TARGET.mode = SOURCE.mode,
            TARGET.speechiness = SOURCE.speechiness,
            TARGET.acousticness = SOURCE.acousticness,
            TARGET.instrumentalness = SOURCE.instrumentalness,
            TARGET.liveness = SOURCE.liveness,
            TARGET.valence = SOURCE.valence,
            TARGET.tempo = SOURCE.tempo,
            TARGET.time_signature = SOURCE.time_signature,
            TARGET.unique_id = SOURCE.unique_id
    """

    merge_query_job = client.query(merge_query)
    rows = merge_query_job.result()
    target_load=f"top 50 data loaded into {target_table}"
    result = result+"\n"+target_load

    return result
