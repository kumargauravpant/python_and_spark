import sys
import glob
import os
import pandas as pd
from pandas.io.parsers import read_csv
from pandas import ExcelFile

folder_list = sys.argv[1]
base_path = '/externaldata01/dev/sams_club/'

with pd.ExcelWriter("/home/prgvk/sams_club/validation.xlsx") as writer:
    for folder in folder_list.split(','):
        print(f'processing {folder}')
        header_file = pd.read_excel('/home/prgvk/sams_club/MADRiD.xls', sheet_name=folder, header=None)
        header_list = list(header_file[1][3:])
    
        os.chdir(os.path.join(base_path, folder))
        files = glob.glob('*')
        
        for file in files[0]:
            if not file.endswith('.gz'):
                df = pd.read_csv(file, sep='|', header=None)
                df.columns = header_list
                # Getting the datatype information
                schema = pd.DataFrame(df.dtypes).reset_index()
                # Checking the not null values
                total = len(df)
                null_count = pd.DataFrame(df.count()/total).reset_index()
                # Checking the -ve numeric values
                numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
                numeric_val = df.select_dtypes(include=numerics)
                neg_per = pd.DataFrame(numeric_val[numeric_val < 0].count()/total).reset_index()
                # Checking the +ve numeric values
                pos_per = pd.DataFrame(numeric_val[numeric_val > 0].count()/total).reset_index()
                # Checking numeric = 0 values
                zero_per = pd.DataFrame(df.isin([0]).sum()/total).reset_index()
                # Joining the datasets together
                print(schema.info())
                print(null_count.info())
                step_1 = pd.merge(left=schema, right=null_count, how='inner', left_on='index', right_on='index')
                step_2 = pd.merge(left=step_1, right=neg_per, how='left', left_on='index', right_on='index')
                step_3 = pd.merge(left=step_2, right=pos_per, how='left', left_on='index', right_on='index')
                final_df = pd.merge(left=step_3, right=zero_per, how='inner', left_on='index', right_on='index')
                final_df.columns = [['column_name','data_type','null_per','neg_per','pos_per','zero_per']]
                print('writing ')
                # Writing results to Execl
                final_df.to_excel(writer, sheet_name=folder)