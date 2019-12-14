'''
ONLY TESTED ON PYTHON3

Unzip daily futures zip files from Barchart ACS, and write them to Postgres.

Usage (from Bash command Line):
Case 1: using local db with no username and password, and only printing the final psql COPY command
$ source barchart_virtualenv/bin/activate
$ python3 step_03_underlying_table_loader.py --zip_folder_parent ./temp_folder/zip_files \
  --begin_yyyy 2011 --end_yyyy 2015  

Case 2: using db with username and password, and executing final psql COPY command
$ source barchart_virtualenv/bin/activate
$ python3 step_03_underlying_table_loader.py --zip_folder_parent ./temp_folder/zip_files \
  --begin_yyyy 2011 --end_yyyy 2015  --db_username mydbusername --db_password mydbpassword --write_to_postgres True

'''
import zipfile
import glob
import pandas as pd
import numpy as np

import sys
import os
p = './'
if  not os.path.abspath(p) in sys.path:
    sys.path.append(os.path.abspath(p))
p = '../'
if  not os.path.abspath(p) in sys.path:
    sys.path.append(os.path.abspath(p))

from barchartacs import db_info
import argparse as ap
import datetime
from tqdm import tqdm
import pathlib
HOME_FOLDER = pathlib.Path.home()
import logging

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

MMM_LIST = {
    1:'jan',
    2:'feb',
    3:'mar',
    4:'apr',
    5:'may',
    6:'jun',
    7:'jul',
    8:'aug',
    9:'sep',
    10:'oct',
    11:'nov',
    12:'dec'
}



if __name__=='__main__':
    parser =  ap.ArgumentParser()
    parser.add_argument('--log_file_path',type=str,
                        help='path to log file. Default = logfile.log',
                        default = 'logfile.log')
    parser.add_argument('--logging_level',type=str,
                        help='log level.  Default = INFO',
                        default = 'INFO')
    parser.add_argument('--zip_folder_parent',type=str,
                        help='full folder path into which you will download zip files')    
    parser.add_argument('--begin_yyyy',type=int,
                        help='The first year of options and futures data to upload to DB (like 2012 or 2016)')
    parser.add_argument('--end_yyyy',type=int,
                        help='The last year of options and futures data to upload to DB (like 2012 or 2016)')
    parser.add_argument('--months_to_include',type=str,
                        help='a comma delimited string of 3 character month names, like jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec.  If not provided all months are included',
                        nargs="?")
    parser.add_argument('--write_to_postgres',type=str,
                        help='if True the data will be written to postgres.  Otherwise, a psql COPY command will be printed to the console.  Default=False',
                        default="False")
#     parser.add_argument('--db_username',type=str,
#                         help='username in Postgres login',
#                         nargs="?")
#     parser.add_argument('--db_password',type=str,
#                         help='password in Postgres login',
#                         nargs="?")
    parser.add_argument('--db_config_csv_path',type=str,
                        help='path to the csv file that holds config_name,dburl,databasename,username,password info for the postgres db that you will update (default is ./postgres_info.csv',
                        default="./postgres_info.csv")
    parser.add_argument('--config_name',type=str,
                        help='value of the config_name column in the db config csv file (default is local',
                        default="local")
    parser.add_argument('--contract_list',type=str,
                        help='a comma delimited string of commodity codes.  Default = CL,CB,ES',
                        default = 'CL,CB,ES')

    args = parser.parse_args()
    
    '''
    ************************** Step 1:  get arguments from argparse variables **************************
    '''
    log_file_path = args.log_file_path
    logging_level = args.logging_level
    logger = init_root_logger(log_file_path, logging_level)
    
    logger.info(str(args))
    

    WRITE_TO_POSTGRES = args.write_to_postgres.lower() == 'true'
    COMMODS_TO_INCLUDE = args.contract_list.split(',')
    BEGIN_YEAR = args.begin_yyyy
    if len(str(BEGIN_YEAR))<=2:
        BEGIN_YEAR +=2000
    END_YEAR = args.end_yyyy
    if len(str(END_YEAR))<=2:
        END_YEAR +=2000



    WRITE_DATA=args.write_to_postgres # set to True if you want to copy new data to postgres using psql copy 
    ZIP_FOLDER_PARENT = args.zip_folder_parent 
    BEGIN_YY = int(str(BEGIN_YEAR)[-2:])
    END_YY =  int(str(END_YEAR)[-2:])
    MONTHS_TO_INCLUDE = args.months_to_include
    if MONTHS_TO_INCLUDE is None:
        MONTHS_TO_INCLUDE = list(MMM_LIST.values())
    else:
        MONTHS_TO_INCLUDE = MONTHS_TO_INCLUDE.split(',')
    
    # Create variables derived from above
    YEARS_TO_INCLUDE = list(np.arange(BEGIN_YY,END_YY+1))
    DB_NAME = 'sec_db'
    SCHEMA_NAME = 'sec_schema'
    UNDERLYING_TABLE_NAME = 'underlying_table'
    FULL_TABLE_NAME = f'{SCHEMA_NAME}.{UNDERLYING_TABLE_NAME}'
    pga = db_info.get_db_info(args.config_name, args.db_config_csv_path)
    DB_USER_NAME = pga.username
    DB_PASSWORD = pga.password
    df_one_rec = pga.get_sql(f"select * from {FULL_TABLE_NAME} limit 1")
    DB_COLUMNS = df_one_rec.columns.values
    
    CSV_TEMP_PATH = './temp_folder/df_all_temp.csv'
    FUTURES_ZIP_FOLDER = f'{ZIP_FOLDER_PARENT}/futures' 
    FUTURES_UNZIP_FOLDER = './temp_folder/unzipfolder_futures'
    if not os.path.isdir(FUTURES_UNZIP_FOLDER):
        logger.info(f'futures unzip folder {FUTURES_UNZIP_FOLDER} being created')
        os.mkdir(FUTURES_UNZIP_FOLDER)
    else:
        logger.info(f'futures unzip folder {FUTURES_UNZIP_FOLDER} already created')
    
    
    '''
    *********************** Step 2:  Unzip futures files, and create single Dataframe called df_all ***********************
    '''
    all_names_ordered = []
    for yy in YEARS_TO_INCLUDE:
        if len(MONTHS_TO_INCLUDE)>0:
            fnames = []
            for mm in MONTHS_TO_INCLUDE:
                txt_file_list = glob.glob(f'{FUTURES_ZIP_FOLDER}/*{mm}{yy}.txt')
                if len(txt_file_list)>0:
                    fnames.append(txt_file_list[0])
                    continue
                possible_paths_to_zip_file = glob.glob(f'{FUTURES_ZIP_FOLDER}/*{mm}{yy}.zip')
                if len(possible_paths_to_zip_file)<=0:
                    logger.warn(f'No futures zip file for {mm} and {yy} exist in {FUTURES_ZIP_FOLDER}')
                    continue
                path_to_zip_file = possible_paths_to_zip_file[0]
                zip_ref = zipfile.ZipFile(path_to_zip_file, 'r')
                zip_ref.extractall(FUTURES_UNZIP_FOLDER)
                zip_ref.close()  
                txt_file = glob.glob(f'{FUTURES_UNZIP_FOLDER}/*{mm}{yy}.txt')[0]
                fnames.append(txt_file)
        
        # see if there was anything to process
        if len(fnames)<=0:
            msg = f'No futures zip files have been found in {FUTURES_ZIP_FOLDER}.  Ending Program'
            raise ValueError(msg)

        d = {}
        for fname in fnames:
            mmm = fname.split('/')[-1].split('.txt')[0][0:-2][-3:]
            d[mmm] = fname
        fnames_ordered = [d[MMM_LIST[m]] for m in MMM_LIST.keys() if MMM_LIST[m] in d]
        all_names_ordered += fnames_ordered
    
    df_all = None
    header = ['contract','month_year','yymmdd','open','high','low','close','volume','open_interest']
    
    for fname in tqdm(all_names_ordered):
        df_temp = pd.read_csv(fname,header=None)
        df_temp.columns = header
        df_temp['commod'] = df_temp.contract.str.slice(0,2)
        df_temp = df_temp[df_temp.commod.isin(COMMODS_TO_INCLUDE)]
        if df_all is None:
            df_all = df_temp.copy()
        else:
            df_all = df_all.append(df_temp,ignore_index=True)
            df_all.index = list(range(len(df_all)))
        
    
    
    '''
    ************* Step 3:  create datatable with column names of database *************
    '''
    df_temp = df_all.copy()#.iloc[:1000]
    isnas = df_temp.yymmdd.isna()
    df_temp = df_temp[~isnas]
    df_temp = df_temp[~df_temp.open_interest.isna()]
    df_temp.volume = df_temp.volume.fillna(0)
    df_temp = df_temp[df_temp.open.astype(str).str.count('\.')<=1]
    df_temp.index = list(range(len(df_temp)))
    df_temp.loc[df_temp.month_year=='Y','month_year'] = '2099Z'
    symbols = df_temp.contract + df_temp.month_year.str.slice(-1,)  + df_temp.month_year.str.slice(2,4)
    settle_dates = ('20' + df_temp.yymmdd.astype(str)).astype(float).astype(int)
    opens = df_temp.open.astype(float)
    highs = df_temp.high.astype(float)
    lows = df_temp.low.astype(float)
    closes = df_temp.close.astype(float)
    volumes = df_temp.volume.astype(int)
    open_interests = df_temp.open_interest.astype(int)
    df_final = pd.DataFrame({'symbol':symbols,
        'settle_date':settle_dates,
        'open':opens,
        'high':highs,
        'low':lows,
        'close':closes,
        'adj_close':closes,
        'volume':volumes,
        'open_interest':open_interests})
    
    # add month_num to df_final
    df_monthnum = pd.read_csv('month_codes.csv')
    dfu2 = df_final.copy()
    dfu2['contract'] = dfu2.symbol.str.slice(0,-3)
    dfu2['year'] = dfu2.symbol.apply(lambda s: 2000 + int(s[-2:]))
    dfu2['month_code'] = dfu2.symbol.str.slice(-3,-2)
    dfu3 = dfu2.merge(df_monthnum,on='month_code',how='inner')
    
    # Create adj_close
    dfu3['yyyymm'] = dfu3.year*100+dfu3.month_num
    dfu4 = dfu3[['contract','symbol','settle_date','yyyymm']]
    dfu4['contract_num'] =dfu4[['contract','settle_date','yyyymm']].groupby(['contract','settle_date']).yyyymm.rank()
    dfu4['contract_num'] = dfu4['contract_num'].astype(int)
    dfu4 = dfu4.sort_values(['settle_date','contract','yyyymm'])
    dfu4.index = list(range(len(dfu4)))
    dfu5 = df_final.merge(dfu4[['symbol','settle_date','contract_num']],on=['symbol','settle_date'])
    dfu5.index = list(range(len(dfu5)))
    dfu5.open=dfu5.open.round(8)
    dfu5.high=dfu5.high.round(8)
    dfu5.low=dfu5.low.round(8)
    dfu5.close=dfu5.close.round(8)
    dfu5.adj_close = dfu5.adj_close.round(8)
    
    # #### Are there dupes??
    ag = ['symbol','settle_date']
    df_counts = dfu5[ag+['close']].groupby(ag,as_index=False).count()
    dupes_exist  = len(df_counts[df_counts.close>1])>0
    if dupes_exist:
        msg = f'The dataframe to be written to the database has duplicate records. They will be dropped'
        logger.warn(msg)
        dfu5 = dfu5.drop_duplicates()
        dfu5.index = list(range(len(dfu5)))
        
    
    
    '''
    ************* Step 4: Write data to a csv to be used by psql COPY *************
    '''
    tbname = FULL_TABLE_NAME
    col_tuple_list =   [('symbol','text'),('settle_date','integer'),('contract_num','integer'),
         ('open','numeric'),('high','numeric'),('low','numeric'),('close','numeric'),
         ('adj_close','numeric'),('volume','integer'),('open_interest','integer')]
    col_list = [l[0] for l in col_tuple_list]
    print(f'creating csv file {CSV_TEMP_PATH}: {datetime.datetime.now()}')
    dfu5[col_list].to_csv(os.path.abspath(CSV_TEMP_PATH),index=False)
    
    '''
    ******** Step 5:  define method to write csv data to postgres efficiently ********
    '''
    def psql_copy():
        global DB_USER_NAME,WRITE_TO_POSTGRES
        copy_cmd = f"\COPY {FULL_TABLE_NAME} FROM '{CSV_TEMP_PATH}' DELIMITER ',' CSV HEADER;"
        if DB_USER_NAME is not None and len(DB_USER_NAME)>0:
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
    ************* Step 6: Write data using psql COPY (or print the psql command) *************
    '''
    psql_copy()
    
    
    '''
    ************************** END **************************
    '''
