
import os, configparser
import pandas as pd
import json
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

    with open(file_full_path,'r', encoding='utf-8') as file:
        content = file.read()
        content = json.loads(content)
        content = [{'Code':key,'Description':values} for key,values in content.items()]
        df = pd.DataFrame(content)

    df_row_count = df.shape[0]
    print(f"INFO  | File records count: {file_row_count}")
    print(f"INFO  | DataFrame records count: {df_row_count}")


    file_stg_path = Path(file_path).parent.parent.joinpath('staging_zone').joinpath(file_name.split('.')[0]+'.csv')
    print(f"INFO  | Staging Zone Path: {file_stg_path}")

    df.to_csv(file_stg_path,index=False)
    print(f"INFO  | File saved successfully in staging zone")




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

    df.to_csv(file_stg_path,index=False)
    print(f"INFO  | File saved successfully in staging zone")

    

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
                read_csv_file(file_path,file_name) 
            elif file_name.split('.')[1] == 'json':
                read_json_file(file_path,file_name)

        elif src == 'api':
            api_key = config[section]['api_key']
            file_stg_path = config[section]['file_stg_path']
            extract_api_data(api_key,file_stg_path)
            
        else:
            print("ERROR | Invalid source type")

if __name__ == '__main__':
    main()