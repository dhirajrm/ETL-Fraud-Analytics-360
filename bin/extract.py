
import os
import pandas as pd
import json
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


        content = file.read()

        print(content)
        #df = json.loads(content)
    #if isinstance(file)

    print(df.head(5))

    
    print(f"INFO  | File records count: {file_row_count}")
    print(json_data[:5])


##
#read_csv_file('C:\ETL_Fraud_360\landing_zone\cards_data','cards_data.csv')
read_json_file('C:\ETL_Fraud_360\landing_zone\mcc_codes','mcc_codes.json')