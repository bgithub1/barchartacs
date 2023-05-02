#!/usr/bin/env python
# coding: utf-8
# %%

# %%


import warnings
warnings.filterwarnings("ignore")


# %%


import pandas as pd
import numpy as np

import sys
import os

if  not os.path.abspath('./') in sys.path:
    sys.path.append(os.path.abspath('./'))
if  not os.path.abspath('../') in sys.path:
    sys.path.append(os.path.abspath('../'))


import datetime
from dateutil.relativedelta import relativedelta
from barchartacs import schedule_it as sch
import pandas_datareader.data as pdr
import yfinance as yf
import traceback
import json
import requests


import pyarrow as pa
import redis

import time
import tqdm

import schedule_it#@UnresolvedImport


# %%


redis_port = 6379
redis_db = redis.Redis(host = 'localhost',port=6379,db=0)


# %%


def dt_to_yyyymmdd(d):
    return int(d.year)*100*100 + int(d.month)*100 + int(d.day)

def str_to_yyyymmdd(d,sep='-'):
    try:
        dt = datetime.datetime.strptime(str(d)[:10],f'%Y{sep}%m{sep}%d')
    except:
        return None
    s = '%04d%02d%02d' %(dt.year,dt.month,dt.day)
    return int(s)

def str_to_date(d,sep='-'):
    try:
        dt = datetime.datetime.strptime(str(d)[:10],f'%Y{sep}%m{sep}%d')
    except:
        return None
    return dt


def fetch_history(symbol,dt_beg,dt_end):
    df = yf.download(symbol, dt_beg, dt_end,threads=False)
    # move index to date column, sort and recreate index
    df['date'] = df.index
    df = df.sort_values('date')
    df.index = list(range(len(df)))
    # make adj close the close
    df = df.drop(['Adj Close'],axis=1)
    cols = df.columns.values 
    cols_dict = {c:c[0].lower() + c[1:] for c in cols}
    df = df.rename(columns = cols_dict)
    df['settle_date'] = df.date.apply(str_to_yyyymmdd)
    df = df.groupby('settle_date',as_index=False).first()
    return df

def get_port_info_values(syms):
    names = syms if type(syms)==list else syms.tolist()
    tickers = yf.Tickers(names)
    dict_list = []
    for n in tqdm.tqdm(names):
        d = tickers.tickers[n].get_info()
        d['symbol'] = n
        dict_list.append(d)
    df_info_values = pd.DataFrame(dict_list)
    return df_info_values
    
def update_wf_port_info(syms):
    try:
#         names = syms if type(syms)==list else syms.tolist()
#         tickers = yf.Tickers(names)
#         dict_list = []
#         for n in tqdm.tqdm(names):
#             d = tickers.tickers[n].get_info()
#             d['symbol'] = n
#             dict_list.append(d)
#         df_info_values = pd.DataFrame(dict_list)
        df_info_values = get_port_info_values(syms)
        info_values_key = 'wf_port_info_csv'
        update_redis_df(info_values_key,df_info_values)
    except Exception as e:
        traceback.print_exc()


def update_redis_df(key,df):
    context = pa.default_serialization_context()#@UndefinedVariable
    redis_db.set(key, context.serialize(df).to_buffer().to_pybytes())


def get_fmp_ratios(symbol):
    ratios_url = f'https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey=5959d0222350b6d05dbfe64794b6f093'
    response = json.loads(requests.get(ratios_url).text)
    return response

def update_db(beg_sym=None,port_path=None):
    syms = None
    if port_path is not None:
        syms = pd.read_csv(port_path).symbol.values
    else:
        sp_url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
        df_sp_members = pd.read_csv(sp_url)  
        df_sp_members = df_sp_members.sort_values('Symbol')
        if beg_sym is not None:
            df_sp_members = df_sp_members[df_sp_members.Symbol>=beg_sym]
        syms = df_sp_members.Symbol.values
    syms = np.append(syms,['SPY','QQQ'])
    data_end_date = datetime.datetime.now()
    data_beg_date = data_end_date - relativedelta(years=5)
    pe_values = []
    closes = []
    with tqdm.tqdm(syms,position=0,leave=True) as pbar:
        for sym in pbar:
            pbar.set_postfix_str(s=sym)
            try:
                df_temp = fetch_history(sym, data_beg_date, data_end_date)
                update_redis_df(f'{sym}_csv',df_temp)
            except Exception as e:
                print(f"ERROR on {sym}: {str(e)}")
        
    update_wf_port_info(syms)

def schedule_updates(t=8,unit='hour',beg_sym=None,port_path=None,num_runs=None):
    logger = schedule_it.init_root_logger("logfile.log", "INFO")
    counter = num_runs
    while True:
        logger.info(f"scheduling update for {unit} {t}")
        sch = schedule_it.ScheduleNext(unit, t,logger = logger)
        sch.wait()
        logger.info(f"updating history")
        update_db(beg_sym=beg_sym,port_path=port_path)
        if counter is not None:
            counter = counter - 1
            if counter <=0:
                return
        logger.info(f"sleeping until next {t} {unit} before next scheduling")
        time.sleep(5*60)


# %%


data_end_date = datetime.datetime.now()
data_beg_date = data_end_date - relativedelta(years=5)

# fetch_history()


# %%


# df2 = fetch_history('FB',data_beg_date, data_end_date)
# df2


# %%


# sys.argv = ['','56','A','../../jupyter_notebooks/wf_port.csv','minute']  


# %%


if __name__=='__main__':
    t = 20 if len(sys.argv)<2 else int(sys.argv[1])
    bs = None if len(sys.argv)<3 else sys.argv[2]
    port_path = None if len(sys.argv)<4 else sys.argv[3]
    unit = 'hour' if len(sys.argv)<5 else sys.argv[4]
    num_runs = 100 if len(sys.argv)<6 else int(sys.argv[5])
    schedule_updates(t=t,unit=unit,beg_sym=bs,port_path=port_path,num_runs=num_runs)


# %%


# !jupyter nbconvert --to script redis_server_stock_data_update.ipynb


# %%





# %%




