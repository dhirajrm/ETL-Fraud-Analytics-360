import pandas as pd
from pathlib import Path
import importlib.util
import os,configparser

class DataTransformer:
    def __init__(self,file,file_path):
        self.file = file
        self.file_path = file_path
        self.data = None

    def read_data(self):
        self.full_file_path = Path(self.file_path).joinpath(self.file)

        chunk_size = 10000
        chunk_list = []

        for chunk in pd.read_csv(self.full_file_path, chunksize=chunk_size ):
            chunk_list.append(chunk)
        self.data = pd.concat(chunk_list,axis=0)

    def correct_dtypes(self):
        print("\nSchema of Data before correction:")
        print(self.data.dtypes)

        self.data = self.data.infer_objects()
            
        print("\nSchema of Data after correction:")
        print(self.data.dtypes)

    def get_metadata(self):
        int_col,date_col,obj_col = [],[],[]
        for column in self.data.columns:
            if self.data[column].dtypes in ["int64","float64"]:
                int_col.append(column) 
            elif self.data[column].dtypes in ["datetime64"]:
                date_col.append(column)
            else:
                obj_col.append(column)

        self.metadata = {"int_col":int_col,"date_col":date_col,"obj_col":obj_col}

    def curate_na(self,action,threshold):
        if action == "drop":
            self.data.dropna(inplace=True,thresh=int(threshold * len(self.data.columns)))
        if action == "fill":
            if self.data.columns in self.metadata["int_col"]:
                self.data[self.data.columns].fillna(self.data[self.data.columns].mean(),inplace=True)
            elif self.data.columns in self.metadata["obj_col"]:
                self.data[self.data.columns].fillna(self.data[self.data.columns].mode(),inplace=True)
            elif self.data.columns in self.metadata["date_col"]:
                self.data[self.data.columns].fillna('2000-01-01',inplace=True)

    def curate_col_names(self):
        self.data.columns = self.data.columns.map(lambda col: col.lower().replace(" ","_"))

    def run_generic_trans(self):
        try:
            self.read_data()
            self.curate_col_names()
            print("INFO | Data read successfully. Sample data as below:")
            print(self.data.head(5))

            self.correct_dtypes()
            self.get_metadata()
            print("INFO |  Data types corrected successfully. Columnar Metadata:")
            print(self.metadata)

            print("INFO |  Running NA curation on the data")
            self.curate_na("drop",0.5)
            print("INFO |  NA curation completed successfully.")

        except Exception as e:
            print(f"ERROR | Error in while running generic transformations on the data: {e}")
            

    def run_xtr(self,file_name):

        xtr_path = Path('C:\ETL_Fraud_360\XTR')
        xtr_file = xtr_path.joinpath('XTR_'+file_name.split('.')[0]+'.py')
                                 
        if os.path.exists(xtr_file):
            print("INFO | Running XTR transformation on the data")
            try:
                spec = importlib.util.spec_from_file_location("XTR",xtr_file)
                module  = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                if hasattr(module,"xtr"):
                    xtr_function = getattr(module,"xtr")
                    if callable(xtr_function):
                        self.data = xtr_function(self.data)
                    else:
                        print(f"ERROR | unable to call XTR from {xtr_file}")
                else:
                    print(f"ERROR | XTR function not found in {xtr_file}")
            
                print("INFO | XTR transformation completed successfully.")

            except Exception as e:
                print(f"ERROR | Error in while running XTR transformations on the data: {e}")
        else:
            print(f"INFO | XTR transformation file not found. hence skipping it for {file_name.split('.')[0]}.")


    def write_data(self,data):
        print(f"INFO  | Writing data to staging zone")        
        df_row_count = data.shape[0]
        print(f"INFO  | Total number of rows in the dataframe: {df_row_count}")
        file_loading_path  = Path(self.file_path).parent.joinpath('loading_zone').joinpath(self.file.split('.')[0]+'.csv')
 
        with open(file_loading_path,'w', newline='') as file:
            for i in range(0,len(data),10000):
                chunk_df = data.iloc[i:i+ 10000]
                chunk_df.to_csv(file_loading_path,index=False, header=(i == 0))

        with open(file_loading_path, 'r') as file:
            file_row_count = sum(1 for _ in file) - 1

        print("INFO  | File saved successfully in loading zone")
        print(f"INFO  | Total number of rows in the file: {file_row_count}")
        if df_row_count == file_row_count:
            print("INFO  | Data loaded successfully in the file")
        else:
            print(f"ERROR | Data loading failed in the file | Data Rows: {df_row_count} <> File Rows: {file_row_count}")


    def generate_ini(self,section,file_name,file_path):
        config = configparser.ConfigParser()

        table_name = 'tbl_'+file_name.split('.')[0]
        file_loading_path = Path(file_path).parent.joinpath('loading_zone').joinpath(file_name.split('.')[0]+'.csv')
        config[section] = {'table_name' : table_name,'file_loading_path' : file_loading_path}
        with open(r'C:\ETL_Fraud_360\conf\load_input.ini','a') as file:
            config.write(file)
        print(f"INFO  | Config file generated successfully")



def main():

    config = configparser.ConfigParser()

    config.read(r"C:\ETL_Fraud_360\conf\transform_input.ini")

    if os.path.exists(r'C:\ETL_Fraud_360\conf\load_input.ini'):
        os.remove(r'C:\ETL_Fraud_360\conf\load_input.ini')

    for section in config.sections():
        print(f"INFO  | Section: {section}")
        
        file_name = str(config[section]['file_name']) if str(config[section]['file_name']).split('.')[1] == 'csv' else str(config[section]['file_name'])+'.csv'
        file_stg_path = Path(config[section]['file_stg_path']).parent if str(Path(config[section]['file_stg_path']).parent).split('\\')[-1] == 'staging_zone' else Path(config[section]['file_stg_path']).parent.joinpath('staging_zone')
        

        print(f"INFO  | File Name: {file_name} | File Staging Path: {file_stg_path}")

        tf = DataTransformer(file_name,file_stg_path)
        try:
            tf.run_generic_trans()
            print("INFO | Generic transformations completed successfully.")

            tf.run_xtr(file_name)
            print("INFO | XTR transformations completed successfully.")
        except Exception as e:
            print(f"ERROR | Error in while running generic transformations on the data: {e}")

        try:
            tf.write_data(tf.data)
            print("INFO | Data written to staging zone successfully.")

            tf.generate_ini(section,file_name,file_stg_path)

        except Exception as e:
            print(f"ERROR | Error in while writing data to the staging zone: {e}")
        
        print("=====================================================================================================")


if __name__ == "__main__":
    main()
        



