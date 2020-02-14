#!/usr/bin/env python
# coding: utf-8

# ## This ipynb will upload daily barchartacs options and futures files
# 1. The notebook will - for both the options table and the futures table - find the last day that there is data, and upload all days from that day.
# 2. If there is more than a months worth of data, you should use steps 1, 2 and 3 to do monthly file uploads

# In[ ]:


import pandas as pd

import argparse as ap
import sys
import os
if  not './' in sys.path:
    sys.path.append('./')
if  not '../' in sys.path:
    sys.path.append('../')

from barchartacs import build_db
from barchartacs import db_info
import datetime
import json
import io
import urllib.request


from tqdm import tqdm,tqdm_notebook


# In[ ]:


CONTRACT_LIST = ['ES','CL','CB','NG']
STRIKE_DIVISOR = 1
CSV_TEMP_PATH_OPTIONS = './temp_folder/df_all_temp_options.csv'
CSV_TEMP_PATH_FUTURES = './temp_folder/df_all_temp_futures.csv'
DIVISOR_DICT = json.load(open('./divisor_dict.json','r'))
OPTTAB = 'sec_schema.options_table'
FUTTAB = 'sec_schema.underlying_table'


# In[ ]:


def psql_copy(pga,full_tablename,csv_temp_path,logger,write_to_postgres=False):
    # first get column names in order as they appear in postgres
    db_username = pga.username
    
    copy_cmd = f"\COPY {full_tablename} FROM '{csv_temp_path}' DELIMITER ',' CSV HEADER;"
    if db_username is not None:
        psql_cmd = f'sudo -u {db_username} psql -d sec_db -c "CMD"'
    else:
        psql_cmd = f'psql  -d sec_db -c "CMD"'
    psql_cmd = psql_cmd.replace('CMD',copy_cmd)
    if  write_to_postgres:  # double check !!!
        logger.info(f'BEGIN executing psql COPY command: {psql_cmd}')
        os.system(psql_cmd)
        logger.info(f'END executing psql COPY command')
    else:
        print(psql_cmd)


# In[ ]:


def do_request(url,dict_headers=None):
    '''
    Example:
    1. get google page
        text_of_webpage =  do_request('https://www.google.com')
    2. get text file from barchart daily 
        barchart_daily_text_file = do_request('http://acs.barchart.com/mri/data/opv07059.csv',dict_headers={"Authorization": "Basic myauthcode"})

    '''
    if dict_headers is None:
        req = urllib.request.Request(url)
    else:
        req = urllib.request.Request(url, headers=dict_headers)
    f = urllib.request.urlopen(req)
    alines = f.read()#.decode('utf-8')
    return alines


# In[ ]:


def build_daily(commod_code_list,yyyymmdd=None,barchart_auth_code=None,text_file_path=None,
               divisor_dict = DIVISOR_DICT):
    '''
    commod_code_list: like ['ES'] or ['CL','CB']
    yyyymmdd: like 20190705.  It must be a date in the current month
    Example:
    df_this_day = build_daily(['ES'],20190705)
    '''
    tfp = './temp_folder/opv.txt' if text_file_path is None else text_file_path
    ymd = yyyymmdd
    if ymd is None:
        # get yesterday
        ymd = int((datetime.datetime.now()-datetime.timedelta(1)).strftime('%Y%m%d'))
    bac = barchart_auth_code
    if bac is None:
        bac = open('./temp_folder/barchart_authcode.txt','r').read()
    y = str(ymd)[3]
    mm = str(ymd)[4:6]
    dd = str(ymd)[6:8]
    opv = 'opv' + mm + dd + y
    url = f'http://acs.barchart.com/mri/data/{opv}.csv'
    dict_header = {"Authorization": f"Basic {bac}"}
    opvtxt = do_request(url,dict_headers=dict_header)
    opvtxt = opvtxt.decode('utf-8')
    open(tfp,'w').write(opvtxt)
    builder = build_db.BuildDb(None,strike_divisor_dict=divisor_dict,
                           contract_list=commod_code_list,write_to_database=False)
    dft = builder.build_options_pg_from_csvs(tfp)
    return dft


# ### Build Futures file for a single day:

# In[ ]:


def get_barchart_acs_single_day_futures_df(yyyymmdd):
    this_year = int(str(yyyymmdd)[0:4])
    zero_year = int(str(yyyymmdd)[0:3])*10
    y = str(this_year - zero_year)
    mm = str(yyyymmdd)[4:6]
    dd = str(yyyymmdd)[6:8]
    fut_url = f'http://acs.barchart.com/mri/data/mrg{mm}{dd}{y}.txt'
    # fut_name = 'fut' + mm + dd + y
    bac = open('./temp_folder/barchart_authcode.txt','r').read()

    dict_header = {"Authorization": f"Basic {bac}"}
    fut_txt = do_request(fut_url,dict_headers=dict_header)
    fut_txt = fut_txt.decode('utf-8').split()
    header = ','.join(['contract','month_year','yymmdd','open','high','low','close','volume','open_interest'])
    fut_lines = [header]+fut_txt
    f = io.StringIO()
    for fut_line in fut_lines:
        f.write(fut_line+'\n')
    f.seek(0)
    df_fut = pd.read_csv(f)
    return df_fut


# In[ ]:


def build_futures_daily(yyyymmdd,logger):
    '''
    ************* Step 1: get file from barchartacs  ************************
    '''
    df_temp = get_barchart_acs_single_day_futures_df(yyyymmdd)
    
    '''
    ************* Step 2:  create DataFrame with column names of database *************
    '''
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
    col_tuple_list =   [('symbol','text'),('settle_date','integer'),('contract_num','integer'),
         ('open','numeric'),('high','numeric'),('low','numeric'),('close','numeric'),
         ('adj_close','numeric'),('volume','integer'),('open_interest','integer')]
    col_list = [l[0] for l in col_tuple_list]
    return dfu5[col_list]
#     print(f'creating csv file {CSV_TEMP_PATH_FUTURES}: {datetime.datetime.now()}')
#     dfu5[col_list].to_csv(os.path.abspath(CSV_TEMP_PATH_FUTURES),index=False)
    


# In[ ]:


def dt_to_yyyymmdd(dt):
    return int(str(dt)[0:4])*100*100 + int(str(dt)[5:7])*100 + int(str(dt)[8:10])

def get_commod_list_from_max_settle_date(pga,tablename=FUTTAB):
    commods_in_last_day_sql = f"""
        with
        f1 as
        (
            select max(settle_date) as maxdate from {tablename}
        )    
        select distinct substring(ft.symbol,1,2) commod from {tablename} ft
        join f1 on f1.maxdate = ft.settle_date
    """
    df_fut_commods_in_last_day = pga.get_sql(commods_in_last_day_sql)
    return list(df_fut_commods_in_last_day.commod.values)

def get_dates_to_fetch(pga,tablename):
    """
    :param tablename: like OPTTAB or FUTTAB
    This method will return a list of yyyymmdd's.  IT WILL NOT RETURN THE CURRENT DAY.
    """
    t = datetime.datetime.now()
    max_yyyymmdd = pga.get_sql(f'select max(settle_date) maxdate from {tablename}').iloc[0].maxdate
    max_year = int(str(max_yyyymmdd)[0:4])
    max_month = int(str(max_yyyymmdd)[4:6])
    max_day = int(str(max_yyyymmdd)[6:8])
    max_dt = datetime.datetime(max_year,max_month,max_day)
    num_days = (t - max_dt).days
    dates_to_process = [max_dt + datetime.timedelta(n) for n in range(1,num_days)]
    yyyymmdds_to_process = [dt_to_yyyymmdd(d) for d in dates_to_process]
    return yyyymmdds_to_process
    

if __name__=='__main__':
    parser =  ap.ArgumentParser()
    parser.add_argument('--log_file_path',type=str,
                        help='path to log file. Default = logfile.log',
                        default = 'logfile.log')
    parser.add_argument('--logging_level',type=str,
                        help='log level.  Default = INFO',
                        default = 'INFO')
    parser.add_argument('--db_config_csv_path',type=str,
                        help='path to the csv file that holds config_name,dburl,databasename,username,password info for the postgres db that you will update (default is ./postgres_info.csv',
                        default="./postgres_info.csv")
    parser.add_argument('--config_name',type=str,
                        help='value of the config_name column in the db config csv file (default is local',
                        default="local")
    parser.add_argument('--contract_list',type=str,
                        help='a comma delimited string of commodity codes.  Default = CL,CB,ES,NG',
                        default = 'CL,CB,ES')
    parser.add_argument('--strike_divisor_json_path',type=str,
                        help='if specified, a path to a json file that contains divisors for each commodity in contract_list',
                        default = './divisor_dict.json')
    parser.add_argument('--write_to_postgres',type=str,
                        help='if True the data will be written to postgres.  Otherwise, a psql COPY command will be printed to the console.  Default=False',
                        default="False")
    args = parser.parse_args()
    
    logger = build_db.init_root_logger(args.log_file_path, args.logging_level)
    pga = db_info.get_db_info(args.config_name, args.db_config_csv_path) 
    WRITE_TO_POSTGRES = str(args.write_to_postgres).lower()=='true'

    
    # create a builder just so that you can get it's logger and it's pga instance
#     builder = build_db.BuildDb(None,strike_divisor_dict=DIVISOR_DICT,
#                            contract_list=CONTRACT_LIST,write_to_database=False)
    logger.info(f"fetching commod lists for options and futures")
    opt_contract_list = get_commod_list_from_max_settle_date(pga,tablename=OPTTAB)
    fut_contract_list = get_commod_list_from_max_settle_date(pga,tablename=FUTTAB)
    
    # get contract list of commod symbols (like CL and ES) for options and futures
    # get first day to start upload
    df_all_options = None
    df_all_futures = None

    yyyymmdds_to_fetch = get_dates_to_fetch(pga,OPTTAB)
    for yyyymmdd in tqdm_notebook(yyyymmdds_to_fetch):
#         yyyymmdd = year*100*100 + month*100 + day
        logger.info(f'executing options build for yyyymmdd {yyyymmdd} at {datetime.datetime.now()}')

        # build options
        try:
            df_temp = build_daily(commod_code_list=opt_contract_list,yyyymmdd=yyyymmdd)
            if df_all_options is None:
                df_all_options = df_temp.copy()
            else:
                df_all_options = df_all_options.append(df_temp)
                df_all_options.index = list(range(len(df_all_options)))
        except Exception as e:
            logger.warn(f'ERROR MAIN LOOP creating options: {str(e)}')

    yyyymmdds_to_fetch = get_dates_to_fetch(pga,FUTTAB)
    for yyyymmdd in tqdm_notebook(yyyymmdds_to_fetch):
        logger.info(f'executing futures build for yyyymmdd {yyyymmdd} at {datetime.datetime.now()}')
        # build futures
        # first get a list of commodity codes (like CL or ES) that are in the most recent day of the database
        commods_in_last_day_sql = """
            select distinct substring(symbol,1,2) commod from {FUTTAB}
            where settle_data = max(settle_date)
        """

        try:
            df_temp = build_futures_daily(yyyymmdd,logger)
            if df_all_futures is None:
                df_all_futures = df_temp.copy()
            else:
                df_all_futures = df_all_futures.append(df_temp)
                df_all_futures.index = list(range(len(df_all_futures)))
        except Exception as e:
            logger.warn(f'ERROR MAIN LOOP creating futures: {str(e)}')

    # write csv files
    # NOW WRITE THIS DATA FOR THIS YEAR
    df_all_options.to_csv(CSV_TEMP_PATH_OPTIONS,index=False)
    # for futures only use those commod codes in fut_contract_list
    df_all_futures = df_all_futures[df_all_futures.symbol.str[0:-3].isin(fut_contract_list)]
    df_all_futures.to_csv(CSV_TEMP_PATH_FUTURES,index=False)



    if WRITE_TO_POSTGRES:
        logger.info(f"MAIN LOOP: writing options data to database")
        abspath = os.path.abspath(CSV_TEMP_PATH_OPTIONS)
        psql_copy(pga,OPTTAB,abspath,write_to_postgres=WRITE_TO_POSTGRES)
        logger.info(f"MAIN LOOP: writing futures data to database")
        abspath = os.path.abspath(CSV_TEMP_PATH_FUTURES)
        psql_copy(pga,FUTTAB,abspath,write_to_postgres=WRITE_TO_POSTGRES)


