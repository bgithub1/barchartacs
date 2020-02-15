'''

Create the options and futures daily settlement table 
   
Overview:
Options and futures daily settlement data is written to a Postgres database with the following characteristics:
 
     
|  Type  |      Name     | 
|--------|---------------|
|Database|   *sec_db*    |
| Schema | *sec_schema*  |
| Table  |*options_table*|
| Table  |*futures_table*|
 
 

Example execution:
source myvirtualenvironment/bin/activate
python3 step_00_futures_table_creator.py --testrun False

 
'''

import sys
import os
if  not os.path.abspath('./') in sys.path:
    sys.path.append(os.path.abspath('./'))
if  not os.path.abspath('../') in sys.path:
    sys.path.append(os.path.abspath('../'))

from barchartacs import build_db
import logging
import argparse as ap
from barchartacs import db_info

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

#  

DB_NAME = 'sec_db'
SCHEMA_NAME = 'sec_schema'

def main(args):
    '''
    ************************** Step 1:  get arguments from argparse variables **************************
    '''
        

    pga = db_info.get_db_info(args.config_name, args.db_config_csv_path)
    DB_USER_NAME = pga.username
    if DB_USER_NAME is not None and len(DB_USER_NAME)<1:
        DB_USER_NAME = None
    DB_PASSWORD = pga.password
    dburl = pga.dburl
    databasename = pga.databasename
    
    '''
    ************************** Step 2:  build an instance of BuildDb **************************
    '''
    bdb = build_db.BuildDb(None,
                  dburl=dburl,
                  databasename=databasename,
                  schema_name=args.schema_name,
                  username=DB_USER_NAME,
                  password=DB_PASSWORD)
    
    '''
    ************************** Step 3:  make a new table, if testorlive==live **************************
    '''
    if str(args.testorlive).lower()=='live':
        print(f'creating schema {bdb.schema_name}')
        bdb.pga.exec_sql_raw(f"create schema IF NOT EXISTS {bdb.schema_name};")
        print(f'creating futures table')
        bdb.make_underlying_table()
        print(f'creating options table')
        bdb.make_options_table()

    
    '''
    ************************* END ****************************
    '''
    logger.info("finished")

if __name__ == '__main__':
    logger = init_root_logger('logger.log','INFO') 
    parser =  ap.ArgumentParser()

    parser.add_argument('--testorlive',type=str,
                        help='if "live", create the table named sec_schema.underlying_table.  Default=test',
                        default="test")
    parser.add_argument('--db_config_csv_path',type=str,
                        help='path to the csv file that holds config_name,dburl,databasename,username,password info for the postgres db that you will update (default is ./postgres_info.csv',
                        default="./postgres_info.csv")
    parser.add_argument('--config_name',type=str,
                        help='value of the config_name column in the db config csv file (default is local',
                        default="local")
    parser.add_argument('--schema_name',type=str,
                        help='schema_name',
                        default=SCHEMA_NAME)
    args = parser.parse_args()
    main(args)
    

