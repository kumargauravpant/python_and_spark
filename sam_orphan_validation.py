import os
import sys
import subprocess
import logging
import pandas as pd
from pandas.io.parsers import read_csv
from pandas import ExcelFile


def get_schema(sheet):
    print(f"fetching the schema information for {sheet}")
    schema_file = pd.read_excel(
        "/opt/supply_chain/dev/sams_club/raw_data_load/MADRiD_Feed_Details.xls",
        sheet_name=sheet,
        header=None,
    )

    tschema = schema_file[3:][1]
    table_schema = [i.lower() for i in tschema]
    print(table_schema)
    return table_schema


def item_orphan_check(table, db):
    print(f'Initiating the item orphan check for {table}')
    table_name = table + "_raw_ext"
    count_query = f'select count(distinct item_nbr) from {db}.{table_name}'
    validation_query = f'''select count(distinct cast(ta.item_nbr as int)) as 
    orphen_Item_count from {db}.{table_name} ta left join (select distinct item_nbr from {db}.item_master_raw_ext) 
    master on cast(ta.item_nbr as int) = cast(master.item_nbr as int) where master.item_nbr is null'''
    
    result_count = subprocess.check_output(['hive', '-e', count_query])
    total_count = result_count.decode("utf-8").rstrip('\n')
    validation_count = subprocess.check_output(['hive', '-e', validation_query])
    orphan_count = validation_count.decode("utf-8").rstrip('\n')
    
    print(f'total_count = {total_count}')
    print(f'orphan_count = {orphan_count}')
        
    return {'Type':'Item', 'Distinct_Count':total_count, 'Orphan_Count':orphan_count, 'Orphan_Per':int(orphan_count)/int(total_count)}
    
    
def vendor_orphan_check(table, db):
    print(f'Initiating the Vendor orphan check for {table}')
    table_name = table + "_raw_ext"
    count_query = f'select count(distinct vendor_nbr) from {db}.{table_name}'
    validation_query = f'''select count(distinct cast(ta.vendor_nbr as int)) as 
    orphen_vendor_count from {db}.{table_name} ta left join (select distinct vendor_number from {db}.vendor_master_raw_ext) 
    master on cast(ta.vendor_nbr as int) = cast(master.vendor_number as int) where master.vendor_number is null'''
    
    result_count = subprocess.check_output(['hive', '-e', count_query])
    total_count = result_count.decode("utf-8").rstrip('\n')
    validation_count = subprocess.check_output(['hive', '-e', validation_query])
    orphan_count = validation_count.decode("utf-8").rstrip('\n')
    
    print(f'total_count = {total_count}')
    print(f'orphan_count = {orphan_count}')
    
    return {'Type':'Vendor', 'Distinct_Count':total_count, 'Orphan_Count':orphan_count, 'Orphan_Per':int(orphan_count)/int(total_count)}
    
def store_orphan_check(table, db):
    print(f'Initiating the Store orphan check for {table}')
    table_name = table + "_raw_ext"
    count_query = f'select count(distinct store_nbr) from {db}.{table_name}'
    validation_query = f'''select count(distinct cast(ta.store_nbr as int)) as 
    orphen_store_count from {db}.{table_name} ta left join (select distinct store_nbr from {db}.store_info_raw_ext) 
    master on cast(ta.store_nbr as int) = cast(master.store_nbr as int) where master.store_nbr is null'''
    
    result_count = subprocess.check_output(['hive', '-e', count_query])
    total_count = result_count.decode("utf-8").rstrip('\n')
    validation_count = subprocess.check_output(['hive', '-e', validation_query])
    orphan_count = validation_count.decode("utf-8").rstrip('\n')
    
    print(f'total_count = {total_count}')
    print(f'orphan_count = {orphan_count}')
    
    return {'Type':'Store', 'Distinct_Count':total_count, 'Orphan_Count':orphan_count, 'Orphan_Per':int(orphan_count)/int(total_count)}
    

def club_orphan_check(table, db):
    print(f'Initiating the Club orphan check for {table}')
    table_name = table + "_raw_ext"
    count_query = f'select count(distinct club_nbr) from {db}.{table_name}'
    validation_query = f'''select count(distinct cast(ta.club_nbr as int)) as 
    orphen_vendor_count from {db}.{table_name} ta left join (select distinct store_nbr from {db}.store_info_raw_ext) 
    master on cast(ta.club_nbr as int) = cast(master.store_nbr as int) where master.store_nbr is null'''
    
    result_count = subprocess.check_output(['hive', '-e', count_query])
    total_count = result_count.decode("utf-8").rstrip('\n')
    validation_count = subprocess.check_output(['hive', '-e', validation_query])
    orphan_count = validation_count.decode("utf-8").rstrip('\n')
    
    print(f'total_count = {total_count}')
    print(f'orphan_count = {orphan_count}')
    
    return {'Type':'Store', 'Distinct_Count':total_count, 'Orphan_Count':orphan_count, 'Orphan_Per':int(orphan_count)/int(total_count)}
    
def dc_orphan_check(table, db):
    print(f'Initiating the DC orphan check for {table}')
    table_name = table + "_raw_ext"
    count_query = f'select count(distinct dc_nbr) from {db}.{table_name}'
    validation_query = f'''select count(distinct cast(ta.dc_nbr as int)) as 
    orphen_vendor_count from {db}.{table_name} ta left join (select distinct dc_nbr from {db}.dc_master_raw_ext) 
    master on cast(ta.dc_nbr as int) = cast(master.dc_nbr as int) where master.dc_nbr is null'''
    
    result_count = subprocess.check_output(['hive', '-e', count_query])
    total_count = result_count.decode("utf-8").rstrip('\n')
    validation_count = subprocess.check_output(['hive', '-e', validation_query])
    orphan_count = validation_count.decode("utf-8").rstrip('\n')
    
    print(f'total_count = {total_count}')
    print(f'orphan_count = {orphan_count}')
    
    return {'Type':'DC', 'Distinct_Count':total_count, 'Orphan_Count':orphan_count, 'Orphan_Per':int(orphan_count)/int(total_count)}
    
    
def main():
    db_name = "sams_supply_chain_external"
    table_list = sys.argv[1]

    with pd.ExcelWriter("/opt/supply_chain/dev/sams_club/raw_data_load/orphan_check.xlsx") as writer:
        for table in table_list.split(','):
            df = pd.DataFrame(columns = ['Type','Distinct_Count','Orphan_Count','Orphan_Per'])
            schema = get_schema(table)

            if "item_nbr" in schema:
                item_dict = item_orphan_check(table, db_name) 
                print(item_dict)
                df = df.append(item_dict, ignore_index = True)
                
            if "vendor_nbr" in schema:
                vendor_dict = vendor_orphan_check(table, db_name)
                print(vendor_dict)
                df = df.append(vendor_dict, ignore_index = True)
            
            if "store_nbr" in schema:
                store_dict = store_orphan_check(table, db_name)
                print(store_dict)
                df = df.append(store_dict, ignore_index = True)
                
            if "club_nbr" in schema:
                club_dict = club_orphan_check(table, db_name)
                print(club_dict)
                df = df.append(club_dict, ignore_index = True)
                
            if "dc_nbr" in schema:
                dc_dict = dc_orphan_check(table, db_name)
                print(dc_dict)
                df = df.append(dc_dict, ignore_index = True)
                
            print(df)
            if len(df) > 0:
                df.to_excel(writer, sheet_name=table)

if __name__ == '__main__':
    main()
