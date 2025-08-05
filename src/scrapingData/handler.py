import json
import re
import requests
from bs4 import BeautifulSoup
import boto3
import os
from datetime import datetime, timezone


def get_bls_data():
    s3 = boto3.client('s3')
    bucket_name = os.environ.get('BLSDATA_BUCKET_NAME')
    bucket_prefix = 'part1/'
    blsDataURl = 'https://download.bls.gov/pub/time.series/pr/'
    headers = {
            'User-Agent': 'EmploymentDataFetcher/1.0 (sanketgohelt1992@gmail.com)',
            'Content-Type': 'text/html'
        }
    response = requests.get(blsDataURl, headers=headers)
    soup = BeautifulSoup(response.text, 'lxml')


    # Step 1: Get list of current files from the BLS page
    pre_tag = soup.find('pre')
    if not pre_tag:
        print('No pre tag found')
        return

    files_dict = {}
    date_matched = False
    time_matched = False
    am_pm_matched = False
    date_pattern = r'\d{1,2}/\d{1,2}/\d{4}'
    time_pattern = r'\d{1,2}:\d{2}'
    am_pm_pattern = r'AM|PM'
    for child in pre_tag.children:
        if child.name == 'br':
            continue
        
        if not child.name:
            for line in child.text.split():
                if not date_matched and not re.match(date_pattern, line):
                    continue
                elif re.match(date_pattern, line):
                    date_str = line
                    date_matched = True
                    continue
                
                if not time_matched and not re.match(time_pattern, line):
                    continue
                elif re.match(time_pattern, line):
                    time_str = line
                    time_matched = True
                    continue
                
                if not am_pm_matched and not re.match(am_pm_pattern, line):
                    continue
                elif re.match(am_pm_pattern, line):
                    am_pm_str = line
                    am_pm_matched = True
                    continue
            
        if child.name == 'a':
            if '[To Parent Directory]' in child.text:
                continue
            if not date_matched or not time_matched or not am_pm_matched:
                print(f'Error in processing date time and am/pm for {child.text}: date_matched={date_matched}, time_matched={time_matched}, am_pm_matched={am_pm_matched}')
                continue
            
            file_name = child.text
            file_update_date = datetime.strptime(f"{date_str} {time_str} {am_pm_str}", "%m/%d/%Y %I:%M %p").replace(tzinfo=timezone.utc)
            files_dict[file_name] = file_update_date
            date_matched = False
            time_matched = False
            am_pm_matched = False


    # Step 2: List existing objects in S3 under the prefix
    list_all_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix = bucket_prefix)
    existing_objects_dict = {}
    if 'Contents' in list_all_objects:
        for obj in list_all_objects['Contents']:
            existing_objects_dict[obj['Key']] = obj['LastModified']
        
    # Step 3: Get list of new/updated/deleted files
    new_files = set()
    updated_files = set()
    deleted_files = set()

    for file_name, _ in existing_objects_dict.items():
        file_name_without_prefix = file_name.replace(bucket_prefix, '')
        if  file_name_without_prefix not in files_dict:
            deleted_files.add(file_name_without_prefix)

    for file_name, file_update_date in files_dict.items():
        file_name_with_prefix = bucket_prefix + file_name
        if file_name_with_prefix in existing_objects_dict and existing_objects_dict[file_name_with_prefix] < file_update_date:
            updated_files.add(file_name)
        elif file_name_with_prefix not in existing_objects_dict:
            new_files.add(file_name)
                
    # Step 4: Make changes to S3
    for file_name in deleted_files:
        s3.delete_object(Bucket=bucket_name, Key=bucket_prefix + file_name)
        print(f'Deleted file {file_name} from S3.')

    for file_name in new_files:
        full_url = blsDataURl + file_name
        try:
            file = requests.get(full_url, headers=headers)
            file.raise_for_status()
            s3.put_object(Bucket=bucket_name, Key=bucket_prefix + file_name, Body=file.text)
            print(f'Created file {file_name} in S3.')
        except Exception as e:
            print(f'Failed to upload {file_name}: {e}')
        
    for file_name in updated_files:
        s3.delete_object(Bucket=bucket_name, Key=bucket_prefix + file_name)
        full_url = blsDataURl + file_name
        try:
            file = requests.get(full_url, headers=headers)
            file.raise_for_status()
            s3.put_object(Bucket=bucket_name, Key=bucket_prefix + file_name, Body=file.text)
            print(f'Updated file {file_name} in S3.')
        except Exception as e:
            print(f'Failed to update {file_name}: {e}')
    
    print(f'Created {len(new_files)} new files, updated {len(updated_files)} files, and deleted {len(deleted_files)} files.')

        
def get_population_data():

    s3 = boto3.client('s3')
    bucket_name = os.environ.get('BLSDATA_BUCKET_NAME')
    bucket_prefix = 'part2/'

    url ='https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population'

    try: 
        # Step 1: Get data from the API
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()

         # Step 2: Convert to JSON string (pretty format optional)
        json_string = json.dumps(json_data, indent=2)

         # Step 3: Define file name (with timestamp)
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%SZ')
        filename = f'population_data_{timestamp}.json'
        s3_key = bucket_prefix + filename

         # Step 4: Upload to S3
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=json_string)
        print(f'Uploaded to S3: {s3_key}')

    except Exception as e:
        print(f'Error: {e}')
    

def handler(event, context):
    get_bls_data()
    get_population_data()