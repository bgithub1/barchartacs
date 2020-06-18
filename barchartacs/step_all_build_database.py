'''

Create the sec_db database, the sec_schema schema, and the tables 
options_table and underlying_table.

Then, populate those tables with data from year beg_yyyy to end_yyyy 
   
Overview:
Options and futures daily settlement data is written to a Postgres database with the following characteristics:
 
     
|  Type  |      Name     | 
|--------|---------------|
|Database|   *sec_db*    |
| Schema | *sec_schema*  |
| Table  |*options_table*|
| Table  |*underlying_table*|

example:

python3 step_all_build_database.py --testorlive live --config_name secdb_aws --acs_username barcu acs_password barcp 
 
'''

import sys
import os
if  not os.path.abspath('./') in sys.path:
    sys.path.append(os.path.abspath('./'))


if  not os.path.abspath('../') in sys.path:
    sys.path.append(os.path.abspath('../'))


from barchartacs import db_info
from barchartacs import build_db
import logging
import argparse as ap
from barchartacs import step_00_create_sec_schema_tables as s00
from pathlib import Path
import pdb

def init_root_logger(logfile='logfile.log',logging_level=None):
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
    parser.add_argument('--acs_username',type=str,
                        help='username of Barchart ACS service')
    parser.add_argument('--acs_password',type=str,
                        help='password of Barchart ACS service')
    parser.add_argument('--zip_folder_parent',type=str,
                        help='full folder path into which you will download zip files',
                        default='./temp_folder/zip_files')
    parser.add_argument('--begin_yy',type=int,
                        help='2 character year like 11 for 2011 or 23 for 2023 for first year of options zip files to download from barchart')
    parser.add_argument('--end_yy',type=int,
                        help='2 character year like 11 for 2011 or 23 for 2023 for last year of options zip files to download from barchart')
    parser.add_argument('--virtualenv_folder',type=str,
                        help='full path to the virtualenv folder',
                        nargs='?')
    parser.add_argument('--run_pdb',type=str,
                        help='if True, then launch pdb',
                        default='False')

    args = parser.parse_args()
    
    
    '''
    ************************** see if db exists and create if it does not **************************
    '''
    run_pdb = str(args.run_pdb).lower()=='true'
        
    db_exists = True
    try:        
        pga = db_info.get_db_info(args.config_name, args.db_config_csv_path)
        pga.get_sql("select * from information_schema.tables where table_schema = 'sec_schema'")
    except:
        db_exists = False
    if not db_exists:
        logger.info('creating sec_db database')
        s_info = db_info.get_db_info_csv(args.config_name, args.db_config_csv_path)
        databasename = s_info.databasename
        username = s_info.username
        password = s_info.password
        cmd = f"sudo -u postgres psql -c 'create database {databasename};'"
        logger.info(f"executing cmd: {cmd}")
        os.system(cmd)
        cmd = f"sudo -u postgres psql -d sec_db -c \"ALTER USER postgres WITH PASSWORD '{password}';\""
        logger.info(f"executing cmd: {cmd}")
        os.system(cmd)
            
    
    '''
    ************************** Step 0: run main in step_00_create_sec_schema_tables **************************
    '''
    s00_args = ap.Namespace(
        testorlive=args.testorlive,
        db_config_csv_path=args.db_config_csv_path,
        config_name=args.config_name,
        schema_name=args.schema_name)              
    s00.main(s00_args)
    
        
    '''
    ************************** Step 1 run main in step_01_download_monthly_acs_files.sh **************************
    '''
    # test to see if /usr/bin/firefox and ./geckodriver exist
    usr_bin_firefox_exists = os.path.isfile('/usr/bin/firefox')
    geckodriver_exists = os.path.isfile('./geckodriver')
    if (not usr_bin_firefox_exists) or (not geckodriver_exists):
        logger.info('getting and installing geckodriver')
        os.system('bash get_geckodriver.sh')
    acsu = args.acs_username
    acsp = args.acs_password
    begyy = args.begin_yy
    endyy = args.end_yy
    zipfold = args.zip_folder_parent
    virt = args.virtualenv_folder
    if virt is None:        
        h = str(Path.home())
        virt = h+'/Virtualenvs3/dashrisk3'
    cmd = f"bash step_01_download_monthly_acs_files.sh {acsu} {acsp} {begyy} {endyy} {zipfold} {virt}"
    logger.info(f"executing cmd: {cmd}")
    if run_pdb:
        pdb.set_trace()
    os.system(cmd)
    
    '''
    ************************** Step 2 run step_02_options_table_loader_single_yyyymm.sh **************************
    '''
    write_to_db = str(args.testorlive).lower()=='live'
    for yy in range(begyy,endyy+1):
        yyyy = 2000 + yy
        for m in range(1,13):
            yyyymm = yyyy * 100 + m
            cmd = f"bash step_02_options_table_loader_single_yyyymm.sh {yyyymm} {zipfold} {write_to_db} {virt} {args.config_name}"
            os.system(cmd)
            
    '''
    ************************** Step 3 run step_03_underlying_table_loader.sh **************************
    '''
    begyyyy = begyy + 2000
    endyyyy = endyy + 2000
    cmd = f"bash step_03_underlying_table_loader.sh {begyyyy} {endyyyy} {zipfold} {write_to_db} {virt} {args.config_name}"
    os.system(cmd)
    
    
    