import json
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from io import StringIO
import os

s3 = boto3.client('s3')
bucket_name = os.environ.get('BLSDATA_BUCKET_NAME')


def clean_dataframe(df):
    """
    Strips whitespace from column names and string values in the DataFrame.
    Converts common columns to correct types where applicable.
    """
    if df is None:
        return df

    # Strip column names
    df.columns = df.columns.str.strip()

    # Strip whitespace from string columns
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

    return df

def loadCurrentFile(bucket_name, prefix='part1/', delimiter='\t'):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            print(f"No files found under prefix '{prefix}'")
            return None

        # Look for a .Current file
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.Current'):
                print(f"Found .Current file: {key}")
                file_obj = s3.get_object(Bucket=bucket_name, Key=key)
                content = file_obj['Body'].read().decode('utf-8')
                df = pd.read_csv(StringIO(content), delimiter=delimiter, skipinitialspace=True)
                print(f"Loaded '{key}' from S3 with shape {df.shape}")
                return df

        print("No .Current file found under prefix.")
        return None

    except ClientError as e:
        print(f"S3 access error: {e}")
        return None
    

def extractS3InfoFromsqsRecord(record) -> tuple:
    """
    Extract bucket name and object key from a single SQS record.
    """
    try:
        body = json.loads(record['Records'][0]['body'])
        bucket_name = body['Records'][0]['s3']['bucket']['name']
        key_name = body['Records'][0]['s3']['object']['key']
        return bucket_name, key_name
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        print(f"Failed to extract bucket/key from SQS record: {e}")
        return None, None
    

def loadJSONFileFromsqsEvent(sqs_record):
    """
    Load JSON file from S3 using info extracted from SQS message and convert to DataFrame.
    """
    bucket, key = extractS3InfoFromsqsRecord(sqs_record)
    if not bucket or not key:
        print(" Missing bucket or key. Skipping.")
        return None

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)

        # Adjust this based on the structure of your JSON
        if isinstance(data, dict) and 'data' in data:
            df = pd.json_normalize(data['data'])
        else:
            df = pd.json_normalize(data)

        print(f"Loaded DataFrame from '{key}' with shape {df.shape}")
        return df

    except Exception as e:
        print(f"Failed to load JSON from S3: {e}")
        return None

#Question 1: 
def calculateMeanAndSTD(dataframe,start_year = 2013, end_year = 2018):
    """
    Computes the mean and standard deviation of U.S. population
    between start_year and end_year (inclusive) from the given DataFrame.

    Parameters:
        df (pd.DataFrame): DataFrame containing columns 'Year' and 'Population'
        start_year (int): Start year of the range (inclusive)
        end_year (int): End year of the range (inclusive)

    Returns:
        dict: {'mean': float, 'std_dev': float}
    """
    try:
        
        dataframe['Year'] = dataframe['Year'].astype(int)
        filtered_df = (dataframe['Year'] >=start_year) & (dataframe['Year']<=end_year)
        filtered_df = dataframe[filtered_df]

        if filtered_df.empty:
            print(f"No data found between years {start_year} and {end_year}.")
            return {'mean': None, 'std_dev': None}

        calculatingMeanPopultion  = filtered_df['Population'].mean()
        calculatingSTDPopultion  = filtered_df['Population'].std()

        return {
            'mean': calculatingMeanPopultion,
            'std_dev': calculatingSTDPopultion
        }
    except Exception as e:
        print(f"Error computing statistics: {e}")
        return {'mean': None, 'std_dev': None}
    
#Question 2:   
def getBestYearPerSeries(timeseriesDF):
    """
    For every series_id in the time-series dataframe, find the year with the largest sum of values.
    Returns a dataframe with: series_id, best year, and max summed value.
    """
    #print("Columns in timeseriesDF:", timeseriesDF.columns.tolist())

    # Ensure proper data types
    timeseriesDF['value'] = pd.to_numeric(timeseriesDF['value'], errors='coerce')
    timeseriesDF['year'] = timeseriesDF['year'].astype(int)
    timeseriesDF['series_id'] = timeseriesDF['series_id'].str.strip()

    # Group by series_id and year, sum values
    yearlySum = timeseriesDF.groupby(['series_id', 'year'])['value'].sum().reset_index()

    # For each series_id, get the year with the max value
    idx = yearlySum.groupby('series_id')['value'].idxmax()
    bestYeardf = yearlySum.loc[idx].reset_index(drop=True)

    return bestYeardf.rename(columns={"series_id":"series_id","year": "best_year", "value": "max_value"}) 


#Question 3: 
def getSeriesWithPopulation(timeseriesDF, populationDF, series_id ,period = 'Q01'):
    """
    Join time-series data (Part 1) and population data (Part 2) for a specific series_id and period.
    Returns a dataframe with: series_id, year, period, value, and Population.
    """

    # Clean string columns
    timeseriesDF['series_id'] = timeseriesDF['series_id'].str.strip()
    timeseriesDF['period'] = timeseriesDF['period'].str.strip()
    populationDF['Year'] = populationDF['Year'].astype(int)

    # Use query for filtering
    filteredTS = timeseriesDF.query("series_id == @series_id and period == @period").copy()

    # Rename for join
    populationDF = populationDF.rename(columns={'Year': 'year'})

    # Merge on 'year'
    result = pd.merge(filteredTS, populationDF[['year', 'Population']], on='year', how='left')

    return result[['series_id', 'year', 'period', 'value', 'Population']]
    

def handler(event, context):
    # Log the event argument for debugging and for use in local development.
    BLSData = loadCurrentFile(bucket_name)
    if BLSData is not None:
        BLSData = clean_dataframe(BLSData)
        print(BLSData.head())
    else:
        print(".Current file not found or failed to load.")

    APIData = loadJSONFileFromsqsEvent(event)
    if APIData is not None:
        APIData = clean_dataframe(APIData)
        print(APIData.head())
    else:
        print("APIData file not found or failed to load.")

     # 2. Get Mean ans Standard Deviation 
    stats = calculateMeanAndSTD(APIData)
    if stats['mean'] is not None and stats['std_dev'] is not None:
        print(f"Mean: {stats['mean']:.2f}, Std Dev: {stats['std_dev']:.2f}")
    else:
        print("Failed to compute mean and standard deviation.")

    # 2. Get best year for each series_id
    bestYearReport = getBestYearPerSeries(BLSData)
    print(bestYearReport)

    #3. Get series_id + population report
    merged_report = getSeriesWithPopulation(BLSData, APIData, series_id='PRS30006032', period='Q01')
    print(merged_report)


