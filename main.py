import base64
import json
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
from google.cloud import storage

def move_old_jpegs(event, context):
    """Moves JPEG files older than a specified age from a source bucket to a destination bucket."""

    # 1. Extract object details from Pub/Sub message
    # pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    # data = json.loads(pubsub_message)
    # bucket_name = data['bucket']
    # file_name = data['name']
    move_from_bucket_name = 'tweeterssp-web-site-contents' 
    move_to_bucket_name = 'archive_jpg_from_birdclassifier'
    age_threshold = timedelta(days=5)  # Adjust the age threshold as needed

    # Connect to Cloud Storage
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(move_from_bucket_name)
    destination_bucket = storage_client.bucket(move_to_bucket_name)  

    # get list of jpegs
    # Filter blobs by filename extension
    jpeg_blobs = source_bucket.list_blobs(prefix='*.jpeg')

    # check file extension and age
    for blob in jpeg_blobs:
        print(blob.name)
        if blob.name.endswith('.jpeg'):  # Check for JPEG files
            if datetime.now(blob.time_created.tzinfo) - blob.time_created > age_threshold:
            # Move the file to the destination bucket
                try:
                    blob.copy_blob(destination_bucket, blob.name)
                    blob.delete()  # Optionally delete the original file
                    print(f"File {blob.name} moved to destination bucket.")
                except Exception as e:
                    print(f"Error moving file: {e}")

