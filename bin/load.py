import pandas as pd
import os, sqlite3, configparser


class DataLoader:

    def __init__(self,table,file_path):
        self.table = table
        self.file_path = file_path
        self.data = None

    def read_data(self):
        chunk_size = 10000
        chunk_list = []

        for chunk in pd.read_csv(self.file_path, chunksize=chunk_size ):
            chunk_list.append(chunk)
        self.data = pd.concat(chunk_list,axis=0)

    def load_table(self,db):
        conn = sqlite3.connect(db)
        self.data.to_sql(self.table, conn, if_exists='replace', index=False)
        conn.close()


def main():

    config = configparser.ConfigParser()

    config.read(r"C:\ETL_Fraud_360\conf\load_input.ini")
    db = r"C:\ETL_Fraud_360\db\fraud_360_db.db"

    for section in config.sections():
        print(f"INFO  | Section: {section}")

        table = config[section]['table_name']
        file_path = config[section]['file_loading_path']

        print(f"INFO  | Table: {table} | File Path: {file_path} | DB: {db}")

        if os.path.exists(file_path):
            loader = DataLoader(table,file_path)
            try:
                loader.read_data()
                print("INFO | Data read successfully from the file")

                try:
                    loader.load_table(db)
                    print("INFO | Data loaded successfully in the table")
                except Exception as e:
                    print(f"ERROR | Error in loading data in the table: {e}")
    
            except Exception as e:
                print(f"ERROR | Error in reading data from the file: {e}")
        else:
            print("ERROR | Invalid file format")

        print("=====================================================================================================")


if __name__ == "__main__":
    main()