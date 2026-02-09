import os
import urllib.request
from google.cloud import storage

CREDENTIALS_FILE = "gcp-creds.json"
BUCKET_NAME = "de-zoomcamp-taxi" 
PROJECT_ID = "de-zoomcamp-2026" 

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_FILE

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

# Jan 2024 to June 2024 (1 to 6)
for i in range(1, 7):
    month = f"{i:02d}"
    file_name = f"yellow_tripdata_2024-{month}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    
    # Download locally
    print(f"Downloading {file_name}...")
    urllib.request.urlretrieve(url, file_name)
    
    # Upload to GCS
    upload_to_gcs(BUCKET_NAME, file_name, file_name)
    
    os.remove(file_name)

print("All files successfully uploaded to GCS!")