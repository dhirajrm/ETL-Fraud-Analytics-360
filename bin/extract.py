
import os, configparser
import pandas as pd
import json,csv
import finnhub
from pathlib import Path


"""
Name: read_csv_file
Functionality: Read the csv file and validate the data
Input: file_path, file_name
Output: None
"""
def read_csv_file(file_path,file_name):

    """File and Data Validation"""

    # check whether there is data in file or not
    file_full_path = os.path.join(file_path,file_name)
    print("INFO  | " f"Pre-extract validation started for file {file_full_path}")
    try:
        file_size_mb = round(os.path.getsize(file_full_path)/1024/1000,3)

        if file_size_mb == 0:
            print("INFO  | " f"The file - {file_full_path} have no records. please check and pass the file with data.")
        else:
            print("INFO  | Data Presence validation - Passed.")
            print("INFO  | " f"File : {file_full_path} | Size: {file_size_mb} MB")

    except FileNotFoundError as e:
        print("ERROR | " f"File not found - {file_full_path}")

    #Get the file record count
    with open(file_full_path,'r') as file:
        file_row_count = sum(1 for _ in file) - 1
    
    print(f"INFO  | File records count: {file_row_count}")


    """Read file in chunk"""
    chunk_size = 10000
    chunk_list = []


    for chunk in pd.read_csv(file_full_path,chunksize=chunk_size):
        chunk_list.append(chunk)

    df = pd.concat(chunk_list,ignore_index=True)

    df_row_count = df.shape[0]

    print(f"INFO  | DataFrame records count: {df_row_count}")

    if file_row_count == df_row_count:
        print(f"INFO  | Dataframe created successfully | {file_row_count} = {df_row_count}")
    else:
        print(f"ERROR | Record count reconciliation failed | {file_row_count} <> {df_row_count}" )

    file_stg_path = Path(file_path).parent.parent.joinpath('staging_zone').joinpath(file_name.split('.')[0]+'.csv')
    print(f"INFO  | Staging Zone Path: {file_stg_path}")

    
    if file_size_mb > 100:
        with open(file_stg_path,'w', newline='') as file:
            for i in range(0,df_row_count,10000):
                chunk_df = df.iloc[i:i+ 10000] 
                chunk_df.to_csv(file,index=False,header=i == 0)
    else:
        df.to_csv(file_stg_path,index=False)

    print(f"INFO  | File saved successfully in staging zone")
    print("=====================================================================================================")


"""
Name: read_json_file   
Functionality: Read the json file and validate the data
Input: file_path, file_name
Output: None
"""   
def read_json_file(file_path,file_name):
    """File and Data Validation"""

    # check whether there is data in file or not
    file_full_path = os.path.join(file_path,file_name)
    print("INFO  | " f"Pre-extract validation started for file {file_full_path}")
    try:
        file_size_mb = round(os.path.getsize(file_full_path)/1024/1000,3)

        if file_size_mb == 0:
            print("INFO  | " f"The file - {file_full_path} have no records. please check and pass the file with data.")
        else:
            print("INFO  | Data Presence validation - Passed.")
            print("INFO  | " f"File : {file_full_path} | Size: {file_size_mb} MB")

    except FileNotFoundError as e:
        print("ERROR | " f"File not found - {file_full_path}")

    #Get the file record count
    with open(file_full_path,'r', encoding='utf-8') as file:
        file_row_count = sum(1 for _ in file) - 1

    try:
        print(f"INFO  | Reading JSON file: {file_full_path}")
        
        # Attempt to read as a standard JSON file
        df = pd.read_json(file_full_path, orient='records')
        print(f"INFO  | Successfully loaded JSON with orient='records'.")

    except ValueError as ve:
        print(f"WARNING | ValueError encountered: {ve}")
        print(f"INFO    | Attempting alternative JSON parsing methods...")

        try:
            # Try reading JSON lines (line-separated JSON objects)
            df = pd.read_json(file_full_path, lines=True)
            print(f"INFO  | Successfully loaded JSON with lines=True.")
        
        except ValueError as ve_lines:
            print(f"WARNING | Failed with lines=True: {ve_lines}")
            
            # Load JSON using the json module to inspect its structure
            try:
                with open(file_full_path, 'r') as file:
                    raw_data = json.load(file)
                    print(f"INFO  | Loaded JSON content using json module: {type(raw_data)}")

                    # Handle scalar JSON (convert to DataFrame)
                    if isinstance(raw_data, dict):
                        df = pd.DataFrame([raw_data])  # Wrap in list for single-row DataFrame
                        print(f"INFO  | Parsed scalar JSON into DataFrame.")
                    
                    # Handle nested JSON or list of records
                    elif isinstance(raw_data, list):
                        df = pd.DataFrame(raw_data)
                        print(f"INFO  | Parsed list JSON into DataFrame.")
                    
                    else:
                        raise ValueError("ERROR | Unsupported JSON structure.")
            
            except Exception as final_ex:
                print(f"ERROR | Failed to parse JSON file: {final_ex}")
    except FileNotFoundError as fnf:
        print(f"ERROR | File not found: {file_full_path} - {fnf}")
        return None
    except Exception as e:
        print(f"ERROR | Unexpected error while reading JSON: {e}")

    df.reset_index(inplace=True)

    df_row_count = df.shape[0]
    print(f"INFO  | File records count: {file_row_count}")
    print(f"INFO  | DataFrame records count: {df_row_count}")


    file_stg_path = Path(file_path).parent.parent.joinpath('staging_zone').joinpath(file_name.split('.')[0]+'.csv')
    print(f"INFO  | Staging Zone Path: {file_stg_path}")

    df_size_mb = df.memory_usage(deep=True).sum() / (1024**2)
    if df_size_mb > 100:
        with open(file_stg_path,'w', newline='') as file:
            for i in range(0,df_row_count,10000):
                chunk_df = df.iloc[i:i+ 10000] 
                chunk_df.to_csv(file,index=False,header=i == 0)
    else:
        df.to_csv(file_stg_path,index=False)
    print(f"INFO  | File saved successfully in staging zone")
    print("=====================================================================================================")



"""
Name: extract_api_data
Functionality: Extract the data from API
Input: api_key
Output: None
"""
def extract_api_data(api_key,file_stg_path):
    data = 'market_status'
    file_stg_path = Path(file_stg_path).joinpath(data+'.csv')
    
    finnhub_client = finnhub.Client(api_key=api_key)

    response = finnhub_client.market_status(exchange=['US','L'])
    print(f"INFO  | API Request accepted and response : {response}")

    if isinstance(response,dict):
        response = [response]

    df  = pd.DataFrame(response)
    df_row_count = df.shape[0]
    print(f"INFO  | DataFrame records count: {df_row_count}")

    print(f"INFO  | Staging Zone Path: {file_stg_path}")

    df_size_mb = df.memory_usage(deep=True).sum() / (1024**2)
    if df_size_mb > 100:
        with open(file_stg_path,'w', newline='') as file:
            for i in range(0,df_row_count,10000):
                chunk_df = df.iloc[i:i+ 10000] 
                chunk_df.to_csv(file,index=False,header=i == 0)
    else:
        df.to_csv(file_stg_path,index=False)
    print(f"INFO  | File saved successfully in staging zone")
    print("=====================================================================================================")

    
"""
Read the input config file trigger the respective function to extract the data.
"""
def main():

    config = configparser.ConfigParser()

    config.read("C:\ETL_Fraud_360\conf\input.conf")

    for section in config.sections():
        print(f"INFO  | Section: {section}")
        
        src = config[section]['src']
        if src == 'file':
            file_path = config[section]['file_path']
            file_name = config[section]['file_name']
            print(f"INFO  | Source: {src} | File Path: {file_path} | File Name: {file_name}")

            if file_name.split('.')[1] == 'csv':
                print("INFO  | CSV File")
                #read_csv_file(file_path,file_name) 
            elif file_name.split('.')[1] == 'json':
                read_json_file(file_path,file_name)

        elif src == 'api':
            api_key = config[section]['api_key']
            file_stg_path = config[section]['file_stg_path']
            #extract_api_data(api_key,file_stg_path)
            
        else:
            print("ERROR | Invalid source type")

if __name__ == '__main__':
    main()