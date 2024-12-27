
import os
import pandas as pd

def read_csv_file(file_path,file_name):

    """File and Data Validation"""

    # check whether there is data in file or not
    full_path = os.path.join(file_path,file_name)
    print("INFO  | " f"Pre-extract validation started for file {full_path}")
    try:
        file_size_mb = round(os.path.getsize(full_path)/1024/1000,3)

        if file_size_mb == 0:
            print("INFO  | " f"The file - {full_path} have no records. please check and pass the file with data.")
        else:
            print("INFO  | Data Presence validation - Passed.")
            print("INFO  | " f"File : {full_path} | Size: {file_size_mb} MB")

    except FileNotFoundError as e:
        print("ERROR | " f"File not found - {full_path}")

    


read_csv_file('C:\ETL_Fraud_360\landing_zone\cards_data','cards_data.csv')