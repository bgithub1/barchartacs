#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import sys
import os
import datetime
from dateutil.relativedelta import relativedelta
import yfinance as yf
import traceback
import json
import requests
from lxml import html
import time
import tqdm


# In[19]:


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
    df['symbol'] = symbol
    newcols = ['symbol'] + [c for c in df.columns.values if c != 'symbol']
    return df[newcols].copy()

def get_futures_list_from_yahoo():
    url = 'https://finance.yahoo.com/commodities'
    resp = requests.get(url)
    df_resp = pd.read_html(resp.text)[0]
    return df_resp

def get_yahoo_history_from_list(
    sym_list,
    beg_dt,
    end_dt,
    use_tqdm=False
):
    '''
    Get hisorical prices from yahoo finance for multiple symbols
    @param sym_list: like ['AAPL','CLH22.CME','ES=F']
    @param beg_dt:  like datetime.datetime.now() -  - relativedelta(years=1)
    @param end_dt:  like datetime.datetime.now()
    @use_tqdm:  show progress bar with symbols
    '''
    df_all = pd.DataFrame()
    if use_tqdm:
        _syms = tqdm.tqdm(sym_list,position=0,leave=True)
        _iterator = tqdm.tqdm(sym_list)
    else:
        _syms = sym_list
        _iterator = _syms
    bad_syms = []
    for sym in _iterator:
        if use_tqdm:
            _syms.set_postfix_str(s=sym)    
        try:
            df = fetch_history(sym,data_beg_date, data_end_date)
            if len(df)>0:
                df_all = df_all.append(df,ignore_index=True)
            else:
                bad_syms.append(sym)
        except:
            bad_syms.append(sym)  
    return df_all,bad_syms


# In[ ]:





# In[ ]:





# In[12]:


if __name__=='__main__':
    save_filename = './temp_folder/df_yahoo_commodities.csv'
    try:
        save_filename_index = sys.argv.index('--save_filename') + 1
        save_filename = sys.argv[save_filename_index]
    except:
        pass
    sym_list = None
    try:
        sym_list_index = sys.argv.index('--sym_list') + 1
        sym_list_string = sys.argv[sym_list_index]
        sym_list = sym_list_string.split(',')
    except:
        df_yahoo_futures_list = get_futures_list_from_yahoo()
        sym_list = df_yahoo_futures_list.Symbol.values
    
    use_tqdm = True
    try:
        use_tqdm_index = sys.argv.index('--use_tqdm') + 1
        use_tqdm = sys.argv[use_tqdm_index].lower() == 'true'
    except:
        pass
    
    data_end_date = datetime.datetime.now()
    data_beg_date = data_end_date - relativedelta(years=1)
    
    df_all,bad_syms = get_yahoo_history_from_list(
        sym_list,data_beg_date,data_end_date,use_tqdm=use_tqdm
    )
    
#     tqdm_syms = tqdm.tqdm(syms,position=0,leave=True)
#     bad_syms = []
#     for sym in tqdm.tqdm(tqdm_syms):
#         tqdm_syms.set_postfix_str(s=sym)    
#         try:
#             df = fetch_history(sym,data_beg_date, data_end_date)
#             if len(df)>0:
#                 df_all = df_all.append(df,ignore_index=True)
#             else:
#                 bad_syms.append(sym)
#         except:
#             bad_syms.append(sym)
    df_all.to_csv(save_filename,index=False)
    print(f"bad_syms: {bad_syms}")
    


# In[ ]:





# In[18]:


# !jupyter nbconvert --to script yahoofinance.ipynb


# In[ ]:




