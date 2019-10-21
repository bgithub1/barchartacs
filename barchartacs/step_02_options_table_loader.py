'''

Write options daily settlement files for a range of years and months to Postgres 
   
Overview:
Options and futures daily settlement data is written to a Postgres database with the following characteristics:
 
     
|  Type  |      Name     | 
|--------|---------------|
|Database|   *sec_db*    |
| Schema | *sec_schema*  |
| Table  |*options_table*|
| Table  |*futures_table*|
 
 
Usage:
1. Set commodities to update in sec_db database
```
CONTRACT_LIST = ['CL','CB','ES']
```
2. If performing an update of the sec_db database for a single month, set the variable `SINGLE_YYYYMM` to a value like `201909` in the second cell below.  Also, set `WRITE_TO_POSTGRES` to `True`
```
SINGLE_YYYYMM = 201909
WRITE_TO_POSTGRES = True
```
3. If performing an update on multiple months, set the variables `BEGIN_YEAR` and `END_YEAR` to values like `2011` and `2020` in the second cell below.  Also, set `WRITE_TO_POSTGRES` to `False`
```
BEGIN_YEAR = 2011
END_YEAR = 2019
WRITE_TO_POSTGRES = False
```
 
'''

import numpy as np
import sys
import os
if  not './' in sys.path:
    sys.path.append('./')
if  not '../' in sys.path:
    sys.path.append('../')

from barchartacs import build_db
import datetime
from tqdm import tqdm
import logging
import argparse as ap
import json

def init_root_logger(logfile,logging_level=None):
    level = logging_level
    if level is None:
        level = logging.DEBUG
    # get root level logger
    logger = logging.getLogger()
    if len(logger.handlers)>0:
        return logger
    logger.setLevel(logging.getLevelName(level))

    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)   
    return logger


DB_NAME = 'sec_db'
SCHEMA_NAME = 'sec_schema'
UNDERLYING_TABLE_NAME = 'options_table'
FULL_TABLE_NAME = f'{SCHEMA_NAME}.{UNDERLYING_TABLE_NAME}'
CSV_TEMP_PATH = os.path.abspath('./temp_folder/df_all_temp.csv')

if __name__ == '__main__':
    parser =  ap.ArgumentParser()
    parser.add_argument('--log_file_path',type=str,
                        help='path to log file. Default = logfile.log',
                        default = 'logfile.log')
    parser.add_argument('--zip_folder_parent',type=str,
                        help='full folder path into which you will download zip files')    
    parser.add_argument('--single_yyyymm',type=int,
                        help='if specified (like 201912), only upload that year and month to DB',
                        nargs="?")
    parser.add_argument('--begin_yyyy',type=int,
                        help='if single_yyyymm is NOT specified the first year of options and futures data to upload to DB (like 2012 or 2016)',
                        nargs="?")
    parser.add_argument('--end_yyyy',type=int,
                        help='if single_yyyymm is NOT specified the last year of options and futures data to upload to DB (like 2012 or 2016)',
                        nargs="?")
    parser.add_argument('--write_to_postgres',type=bool,
                        help='if --write_to_postgres appears on the command line, the data will be written to postgres.  Otherwise, a psql COPY command will be printed to the console',)
    parser.add_argument('--db_username',type=str,
                        help='username in Postgres login',
                        default = 'logfile.log')
    parser.add_argument('--db_password',type=str,
                        help='password in Postgres login',
                        default = 'logfile.log')
    parser.add_argument('--contract_list',type=str,
                        help='a comma delimited string of commodity codes.  Default = CL,CB,ES',
                        default = 'CL,CB,ES')
    parser.add_argument('--strike_divisor_json_path',type=str,
                        help='if specified, a path to a json file that contains divisors for each commodity in contract_list',
                        default = './divisor_dict.json')
    parser.add_argument('--show_browser',type=bool,
                        help='if --show_browser is on the command line, then the browser will be shown during scraping')
    parser.add_argument('--logging_level',type=str,
                        help='log level.  Default = INFO',
                        default = 'INFO')
    args = parser.parse_args()
    
    '''
    ************************** Step 1:  get arguments from argparse variables **************************
    '''
    log_file_path = args.log_file_path
    logging_level = args.logging_level
    logger = init_root_logger(log_file_path, logging_level)
    
    

    SINGLE_YYYYMM = args.single_yyyymm
    WRITE_TO_POSTGRES = True if args.write_to_postgres else False
    CONTRACT_LIST = args.contract_list.split(',')
    STRIKE_DIVISOR_DICT = json.load(open(args.strike_divisor_json_path,'r'))
    BEGIN_YEAR = args.begin_yyyy
    if len(str(BEGIN_YEAR))<=2:
        BEGIN_YEAR +=2000
    END_YEAR = args.end_yyyy
    if len(str(END_YEAR))<=2:
        END_YEAR +=2000
    DB_USER_NAME = None
    options_folder  = args.zip_folder_parent + "/options"
    futures_folder  = args.zip_folder_parent + "/futures"

    
    '''
    ************************** Step 2:  define method to write csv data to postgres efficiently **************************
    '''
    def psql_copy():
        global DB_USER_NAME,WRITE_TO_POSTGRES
        copy_cmd = f"\COPY {FULL_TABLE_NAME} FROM '{CSV_TEMP_PATH}' DELIMITER ',' CSV HEADER;"
        if DB_USER_NAME is not None:
            psql_cmd = f'sudo -u {DB_USER_NAME} psql -d testdb -c "CMD"'
        else:
            psql_cmd = f'psql  -d sec_db -c "CMD"'
        psql_cmd = psql_cmd.replace('CMD',copy_cmd)
        if  WRITE_TO_POSTGRES:  # double check !!!
            logger.info(f'BEGIN executing psql COPY command')
            os.system(psql_cmd)
            logger.info(f'END executing psql COPY command')
        else:
            print(psql_cmd)
     
    
    
    '''
    ****** Step 3:  if SINGLE_YYYYMM is None then execute a range of years *******
    '''
    if SINGLE_YYYYMM is None:
        df_all = None
    
        # for yyyy in tqdm_notebook(np.arange(2011,2020)):
        for yyyy in tqdm(np.arange(BEGIN_YEAR,END_YEAR+1)):
            for mm in tqdm(np.arange(1,13)):
                yyyymm = yyyy*100 + mm
                bdb = build_db.BuildDb(options_folder,yyyymm,strike_divisor_dict=STRIKE_DIVISOR_DICT,
                                       contract_list=CONTRACT_LIST,write_to_database=False)
                try:
                    df_temp = bdb.execute()            
                    if df_all is None:
                        df_all = df_temp.copy()
                    else:
                        df_all = df_all.append(df_temp)
                        df_all.index = list(range(len(df_all)))
                except Exception as e:
                    bdb.logger.warn(f'ERROR MAIN LOOP: {str(e)}')
    
        # write all data to a csv file, that will be used in the postgres COPY command
        df_all.to_csv(CSV_TEMP_PATH,index=False)
        
    '''
    ****** Step 4:  if SINGLE_YYYYMM is NOT None then execute a specfic year and month *******
    '''
    if SINGLE_YYYYMM is not None:
        yyyymm = SINGLE_YYYYMM
        df_single = None
    
        bdb = build_db.BuildDb(options_folder,yyyymm,strike_divisor_dict=STRIKE_DIVISOR_DICT,
                               contract_list=CONTRACT_LIST,write_to_database=False)
        try:
            df_temp = bdb.execute()            
            if df_single is None:
                df_single = df_temp.copy()
            else:
                df_single = df_single.append(df_temp)
                df_single.index = list(range(len(df_single)))
        except Exception as e:
            bdb.logger.warn(f'ERROR MAIN LOOP: {str(e)}')# NOW WRITE THIS DATA FOR THIS YEAR
        df_single.to_csv(CSV_TEMP_PATH,index=False)
    
    
    '''
    ****** Step 5:  Either write data to Postgres, or print COPY command *******
    '''
    psql_copy()
    
    '''
    ************************* END ****************************
    '''
    logger.info("finished")
    

