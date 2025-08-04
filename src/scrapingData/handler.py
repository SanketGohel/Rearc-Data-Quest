import json

import requests
from bs4 import BeautifulSoup
import boto3
import os
from datetime import datetime


def blsData():
    s3 = boto3.client('s3')
    bucket_name = os.environ.get('BLSDATA_BUCKET_NAME')
    prefix = 'part1/'
    blsDataURl = "https://download.bls.gov/pub/time.series/pr/"
    headers = {
            'User-Agent': 'EmploymentDataFetcher/1.0 (sanketgohelt1992@gmail.com)',
            'Content-Type': 'text/html'
        }
    response = requests.get(blsDataURl, headers=headers)
    soup = BeautifulSoup(response.text, 'lxml')
    
    # Step 1: Get list of current files from the BLS page
    current_title = set()
    for tag in soup.find_all('a'):
        title = tag.text.strip()
        current_title.add(title)

    # Step 2: List existing objects in S3 under the prefix
    listAllObjects = s3.list_objects_v2(Bucket=bucket_name, Prefix = prefix)
    existing_object  = set()
    if 'Contents' in listAllObjects:
        for obj in listAllObjects['Contents']:
            key = obj['Key']
            existing_object.add(key)

    # Step 3: Upload new/changed files
    for title in current_title:
        key = prefix + title
        if key in existing_object:
            print(f"Skipped '{title}': already exists in S3.")
            continue

        full_url = blsDataURl + title
        try:
            file = requests.get(full_url, headers=headers)
            file.raise_for_status()
            s3.put_object(Bucket=bucket_name, Key=key, Body=file.text)
            print(f"Object '{title}' uploaded successfully.")
        except Exception as e:
            print(f"Failed to upload '{title}': {e}")

    # Step 4: Delete S3 files that are no longer present on the site

    object_to_keep = set()
    for title in current_title:
        object_to_keep.add(prefix + title)
    objects_to_delete = existing_object - object_to_keep
    for key in objects_to_delete:
        try:
            s3.delete_object(Bucket=bucket_name, Key=key)
            print(f"Deleted obsolete file '{key}' from S3.")
        except:
            print(f"Failed to delete '{key}': {e}")

        
def populationData():

    # Setup
    s3 = boto3.client('s3')
    bucket_name = os.environ.get('BLSDATA_BUCKET_NAME')
    s3_key_prefix = 'part2/'

    url ='https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population'

    try: 
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()

         # Step 2: Convert to JSON string (pretty format optional)
        json_string = json.dumps(json_data, indent=2)

         # Step 3: Define file name (with timestamp)
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
        filename = f"population_data_{timestamp}.json"
        s3_key = f"part2/{filename}" 

         # Step 4: Upload to S3
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=json_string)
        print(f"Uploaded to S3: {s3_key}")

    except Exception as e:
        print(f"Error: {e}")
    



def handler(event, context):
    # Log the event argument for debugging and for use in local development.
    blsData()
    populationData()