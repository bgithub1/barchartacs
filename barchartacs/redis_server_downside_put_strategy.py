#!/usr/bin/env python
# coding: utf-8

# ### Redis server to support Dashapps in this folder

# ## IF YOU WANT TO SEE WARNINGS, COMMENT THIS OUT

# In[1]:


import warnings
warnings.filterwarnings("ignore")


# In[2]:


import pandas as pd

import sys
import os

if  not os.path.abspath('./') in sys.path:
    sys.path.append(os.path.abspath('./'))
if  not os.path.abspath('../') in sys.path:
    sys.path.append(os.path.abspath('../'))


import datetime
from dateutil.relativedelta import relativedelta


from barchartacs import pg_pandas as pg
from barchartacs import schedule_it as sch

import mibian
import py_vollib
import importlib
from py_vollib import black
from py_vollib.black import implied_volatility

import ipdb,pdb
import traceback

import pandas_datareader.data as pdr
from scipy.stats import norm

import pyarrow as pa
import redis

import time
import schedule_it#@UnresolvedImport

# In[3]:


redis_port = 6379
redis_db = redis.Redis(host = 'localhost',port=6379,db=0)


# #### Step 01: define important functions that are used below

# In[4]:


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
    df = pdr.DataReader(symbol, 'yahoo', dt_beg, dt_end)
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
    return df

def update_redis_df(key,df):
    context = pa.default_serialization_context()#@UndefinedVariable
    redis_db.set(key, context.serialize(df).to_buffer().to_pybytes())


# #### Step 02: get Initial Data

# In[5]:


def update_db():
    sp_data_end_date = datetime.datetime.now()
    sp_data_beg_date = sp_data_end_date - relativedelta(years=30)
    beg_date_str = datetime.datetime.strftime(sp_data_beg_date,'%Y-%m-%d')
    end_date_str = datetime.datetime.strftime(sp_data_end_date,'%Y-%m-%d')
    df_init_dates = pd.DataFrame({'sp_data_end_date':[sp_data_end_date],
                                 'sp_data_beg_date':[sp_data_beg_date],
                                 'beg_date_str':[beg_date_str],
                                 'end_date_str':[end_date_str]})

    df_spy = fetch_history('^GSPC', sp_data_beg_date, sp_data_end_date)
    update_redis_df('df_spy',df_spy)
    
    df_vix = fetch_history('^VIX',sp_data_beg_date,sp_data_end_date)
    update_redis_df('df_vix',df_vix)
    
    df_tnx = fetch_history('^TNX',sp_data_beg_date,sp_data_end_date)
    update_redis_df('df_tnx',df_tnx)

    fred_url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1168&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=DGS1&scale=left&cosd=1962-01-02&coed=2021-05-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Daily&fam=avg&fgst=lin&fgsnd=2009-06-01&line_index=1&transformation=lin&vintage_date=2021-05-04&revision_date=2021-05-04&nd=1962-01-02'
    df_1yr_rate = pd.read_csv(fred_url)
    dates_1yr = [datetime.datetime.strptime(d,'%Y-%m-%d') for d in df_1yr_rate.DATE.values]
    df_1yr_rate['settle_date'] = [int(d.year)*100*100+int(d.month)*100+int(d.day) for d in dates_1yr]
    df_1yr_rate = df_1yr_rate.rename(columns={'DGS1':'rate'})
    df_1yr_rate = df_1yr_rate[['settle_date','rate']]
    df_1yr_rate.rate = [0.0 if s=='.' else float(s) / 100 for s in df_1yr_rate.rate]
    df_1yr_rate['prev'] = df_1yr_rate.rate.rolling(5).mean()
    df_1yr_rate.rate = df_1yr_rate.apply(lambda r:r.prev if r.rate==0 else r.rate,axis=1)
    # add misssing final dates to df_1yr_rate
    sd1max = df_1yr_rate.settle_date.max()
    settle_dates_to_append = df_spy[df_spy.settle_date>sd1max].settle_date.values
    s1_last = df_1yr_rate.iloc[-1]
    for sdta in settle_dates_to_append:
        s1_last_copy = s1_last.copy()
        s1_last_copy['settle_date'] = int(sdta)
        df_1yr_rate = df_1yr_rate.append(s1_last_copy,ignore_index=True)
    df_1yr_rate.settle_date = df_1yr_rate.settle_date.astype(int)
    update_redis_df('df_1yr_rate',df_1yr_rate)

    df_div = pd.read_csv('sp_div_yield.csv')
    update_redis_df('df_div',df_div)
    


# In[6]:


def schedule_updates(h=8):
    logger = schedule_it.init_root_logger("logfile.log", "INFO")
    while True:
        logger.info(f"scheduling update for hour {h}")
        sch = schedule_it.ScheduleNext('hour', h,logger = logger)
        sch.wait()
        logger.info(f"updating history")
        update_db()
        logger.info(f"sleeping for an hour before next scheduling")
        time.sleep(60*60)
    


# In[ ]:


if __name__=='__main__':
    h = 20 if len(sys.argv)<2 else int(sys.argv[1])
#     update_db()
    schedule_updates(h)


# ## END

# In[ ]:




