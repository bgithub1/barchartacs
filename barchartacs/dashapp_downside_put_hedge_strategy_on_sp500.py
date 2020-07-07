#!/usr/bin/env python
# coding: utf-8

# ## Turn your S&P500 portfolio into synthetic in-the-money calls.
# 
# #### This notebook researches a strategy in which:
# 1. You buy the S&P 500 (using an ETF);
# 2. You choose the value ```put_perc_otm```, which is the **percent below the S&P purchase price** to use as the strike price of a put that limits your downside exposure;
# 3. You choose the value ```years_to_hedge```, which is the duration of the put;
# 4. Actions to take as the S&P price moves and time passes:
#   * S&P rises to  ``` 2 * put_perc_otm``` above current hedge strike: 
#     * you sell the previous put, and purhase another put at a **higher** strike, and for the full ```years_to_hedge```, effectively buying diagonal put spreads
#   * S&P falls to ```2 * put_perc_otm``` below the current hedge strike: 
#     * you sell the previous put, and purhase another put at a **lower** strike, and for the full ```years_to_hedge```, effectively selling diagonal put spreads
# 
# #### The main benefit of this strategy
# * The strategy is designed to provide continual insurance  of your long S&P position, using a rolling series of puts.  These puts effectively turn your S&P position into a call that still collects dividends.
# * Depending on where the price of the S&P 500 is relative to the current put strike, you will either have a position that is long an in-the-money call (as the S&P 500 rises to newer all time highs, or an out of the money all (as the S&P falls from those all time highs).
# 

# ## Create a hedge strategy and use data on ^GSPC from yahoo 

# ### Calculate the cost/revenue of the hedge.
# The put hedge that you will buy will initially be below the current SP price by a percentage which you set in the variable ```put_perc_otm```.  When the price of the SP rises high enough so that you can raise the strike price of the hedge, you sell the current put (if there is any value in it) and buy a new put that is ```put_perc_otm``` percent higher than the previous put.  In this way, you are not letting your hedge get too far from the money.
# 
# 
# * Remember that, since you are comparing this put strategy to "Buy-And-Hold"
#   * Rolls to a higher strike are a cost to the strategy
#   * Rolls to a lower strike are revenue to the strategy.

# ## IF YOU WANT TO SEE WARNINGS, COMMENT THIS OUT

# In[1]:


import warnings
warnings.filterwarnings("ignore")


# In[2]:


# import zipfile
# import glob
import pandas as pd
import numpy as np

# from argparse import ArgumentParser
# from argparse import RawDescriptionHelpFormatter
import sys
import os
if  not os.path.abspath('./') in sys.path:
    sys.path.append(os.path.abspath('./'))
if  not os.path.abspath(',./') in sys.path:
    sys.path.append(os.path.abspath('../'))

# from IPython.core.display import  HTML
# from barchartacs import build_db
# from barchartacs import db_info
import plotly.graph_objs as go
# from plotly.offline import  init_notebook_mode, iplot
# init_notebook_mode(connected=True)
# import plotly.tools as tls

# from plotly.graph_objs.layout import Font,Margin,Modebar
# from IPython import display

import datetime
# from dateutil.relativedelta import relativedelta
# import io
from tqdm import tqdm
# from barchartacs import pg_pandas as pg
# import mibian
# import py_vollib
from py_vollib import black
# from py_vollib.black import implied_volatility
import pdb
# import traceback
import pandas_datareader.data as pdr
# from scipy.stats import norm

# from ipysheet import from_dataframe,to_dataframe
from dashapp import dashapp2 as dashapp
# import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.express as px

import pyarrow as pa
import redis


# ### Open a redis port.  This implies that a redis server is running.
# ##### see the ipynb notebook ```redis_server.ipynb```

# In[3]:


redis_port = 6379
redis_db = redis.Redis(host = 'localhost',port=6379,db=0)


# In[4]:


def get_redis_df(key):
    context = pa.default_serialization_context()
    df = context.deserialize(redis_db.get(key))
    return df


# #### Step 01: define important functions that are used below

# In[5]:


# def dt_to_yyyymmdd(d):
#     return int(d.year)*100*100 + int(d.month)*100 + int(d.day)

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


# def fetch_history(symbol,dt_beg,dt_end):
#     df = pdr.DataReader(symbol, 'yahoo', dt_beg, dt_end)
#     # move index to date column, sort and recreate index
#     df['date'] = df.index
#     df = df.sort_values('date')
#     df.index = list(range(len(df)))
#     # make adj close the close
#     df = df.drop(['Adj Close'],axis=1)
#     cols = df.columns.values 
#     cols_dict = {c:c[0].lower() + c[1:] for c in cols}
#     df = df.rename(columns = cols_dict)
#     df['settle_date'] = df.date.apply(str_to_yyyymmdd)
#     return df


# In[6]:


def plotly_plot(df_in,x_column,plot_title=None,
                y_left_label=None,y_right_label=None,
                bar_plot=False,width=800,height=400,
                number_of_ticks_display=20,
                yaxis2_cols=None,
                x_value_labels=None,
                modebar_orientation='v',modebar_color='grey',
                legend_x=None,legend_y=None):
    ya2c = [] if yaxis2_cols is None else yaxis2_cols
    ycols = [c for c in df_in.columns.values if c != x_column]
    # create tdvals, which will have x axis labels
    td = list(df_in[x_column]) 
    nt = len(df_in)-1 if number_of_ticks_display > len(df_in) else number_of_ticks_display
    spacing = len(td)//nt
    tdvals = td[::spacing]
    tdtext = tdvals
    if x_value_labels is not None:
        tdtext = [x_value_labels[i] for i in tdvals]
    
    # create data for graph
    data = []
    # iterate through all ycols to append to data that gets passed to go.Figure
    for ycol in ycols:
        if bar_plot:
            b = go.Bar(x=td,y=df_in[ycol],name=ycol,yaxis='y' if ycol not in ya2c else 'y2')
        else:
            b = go.Scatter(x=td,y=df_in[ycol],name=ycol,yaxis='y' if ycol not in ya2c else 'y2')
        data.append(b)

    # create a layout

    layout = go.Layout(
        title=plot_title,
        xaxis=dict(
            ticktext=tdtext,
            tickvals=tdvals,
            tickangle=45,
            type='category'),
        yaxis=dict(
            title='y main' if y_left_label is None else y_left_label
        ),
        yaxis2=dict(
            title='y alt' if y_right_label is None else y_right_label,
            overlaying='y',
            side='right'),
        autosize=True,
#         autosize=False,
#         width=width,
#         height=height,
        margin=Margin(
            b=100
        ),
        modebar={'orientation': modebar_orientation,'bgcolor':modebar_color}
    )

    fig = go.Figure(data=data,layout=layout)
    fig.update_layout(
        title={
            'text': plot_title,
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    if (legend_x is not None) and (legend_y is not None):
        fig.update_layout(legend=dict(x=legend_x, y=legend_y))
    return fig

def plotly_shaded_rectangles(beg_end_date_tuple_list,fig):
    ld_shapes = []
    for beg_end_date_tuple in beg_end_date_tuple_list:
        ld_beg = beg_end_date_tuple[0]
        ld_end = beg_end_date_tuple[1]
        ld_shape = dict(
            type="rect",
            # x-reference is assigned to the x-values
            xref="x",
            # y-reference is assigned to the plot paper [0,1]
            yref="paper",
#             x0=ld_beg[i],
            x0=ld_beg,
            y0=0,
#             x1=ld_end[i],
            x1=ld_end,
            y1=1,
            fillcolor="LightSalmon",
            opacity=0.5,
            layer="below",
            line_width=0,
        )
        ld_shapes.append(ld_shape)

    fig.update_layout(shapes=ld_shapes)
    return fig


# In[7]:


def calc_put_spread(
    atm_vol,current_hedge_strike,prev_hedge_strike,
    hedge_date,prev_hedge_date,rate,put_perc_otm,years_to_hedge):
    '''
    !! This should only be exexuted on rows of dft where dft.time_to_hedge==True !!

    Calculate the value of the option spread where the legs are: 
      1. the current_hedge_strike 
      2. previous hedge strike
    The value will be positive if you are buying the spread b/c you are rolling
      the previous hedge forward (to a higher strike).
    The value will be negative if you are selling the spread b/c you are rolling
      the previous hedge backward (to a lower strike)
    '''
     #black.black(flag, F, K, t, r, sigma)
    atm_vol = atm_vol
    if (np.isnan(prev_hedge_strike)) or (prev_hedge_strike < current_hedge_strike):
        curr_strike_vol = atm_vol + .04 
        prev_strike_vol = atm_vol + .08
    else:
        curr_strike_vol = atm_vol - .04 
        prev_strike_vol = atm_vol - .06

    days_left_in_prev_hedge = (hedge_date - prev_hedge_date).days

    # calculate remaining of previous hedge
    if np.isnan(prev_hedge_strike):
        underlying_price = current_hedge_strike * (1+put_perc_otm)
        curr_hedge =  black.black('p', underlying_price, current_hedge_strike, years_to_hedge,rate, curr_strike_vol)
        remaining_opt_value = 0
    elif prev_hedge_strike < current_hedge_strike:
        # we are rolling up b/c the market is put_perc_otm ABOVE the current_hedge
        underlying_price = current_hedge_strike * (1+put_perc_otm)
        curr_hedge =  black.black('p', underlying_price, current_hedge_strike, years_to_hedge,rate, curr_strike_vol)
        if days_left_in_prev_hedge > years_to_hedge*365:
            remaining_opt_value = 0
        else:
            time_remaining = days_left_in_prev_hedge/(years_to_hedge*365)
            remaining_opt_value = black.black('p', underlying_price, prev_hedge_strike, 
                                              time_remaining, rate, prev_strike_vol)
    else:
        # we are rolling down b/c the market is put_perc_otm BELOW the current_hedge
        underlying_price = current_hedge_strike * (1-put_perc_otm)
        curr_hedge =  black.black('p', underlying_price, current_hedge_strike, years_to_hedge, rate, curr_strike_vol)
        if days_left_in_prev_hedge > years_to_hedge*365:
            remaining_opt_value = prev_hedge_strike - underlying_price
        else:
            remaining_opt_value =  black.black('p', underlying_price, prev_hedge_strike, years_to_hedge, rate, prev_strike_vol)


    return curr_hedge - remaining_opt_value


# #### Step 02: Define methods that creates the dataframe called ```dft``` which has all of the strategy info, incluing hedge values.
# 
# 

# In[8]:
class DataInputs():
    def __init__(self):
        self.df_spy = get_redis_df('df_spy')
        self.df_vix = get_redis_df('df_vix')
        self.df_1yr_rate = get_redis_df('df_1yr_rate')
        self.df_div = get_redis_df('df_div')


# def create_dft(put_perc_otm,years_to_hedge,
#               yyyymmdd_beg=None,yyyymmdd_end=None,use_fast=True):
# #     global df_spy,df_1yr_rate,df_vix,df_div
#     df_spy = get_redis_df('df_spy')
#     df_vix = get_redis_df('df_vix')
# #     df_tnx = get_redis_df('df_tnx')
#     df_1yr_rate = get_redis_df('df_1yr_rate')
#     df_div = get_redis_df('df_div')
def create_dft(put_perc_otm,years_to_hedge,
              yyyymmdd_beg=None,yyyymmdd_end=None,use_fast=True,
              data_inputs=None):
    if data_inputs is None:
        df_spy = get_redis_df('df_spy')
        df_vix = get_redis_df('df_vix')
        df_1yr_rate = get_redis_df('df_1yr_rate')
        df_div = get_redis_df('df_div')
    else:
        df_spy = data_inputs.df_spy
        df_vix = data_inputs.df_vix
        df_1yr_rate = data_inputs.df_1yr_rate
        df_div = data_inputs.df_div
        

            
    # Create a lambda that converts yyyymmdd integer to a datetime object
    yyyymmdd_to_dt = lambda v:datetime.datetime(
            int(str(v)[0:4]),int(str(v)[4:6]),int(str(v)[6:8])
    )

    # Grab only the relevant columns from df_spy
    dft = df_spy[['settle_date','close','high','low']]
    if yyyymmdd_beg is not None:
        dft = dft[dft.settle_date>=yyyymmdd_beg]
    if yyyymmdd_end is not None:
        dft = dft[dft.settle_date<=yyyymmdd_end]
        
    # Create a datetime settle date, along with the yyyymmdd settle_date column
    dft['settle_dt'] = dft.settle_date.apply(yyyymmdd_to_dt)
#     print(f"create_dft inputs:{put_perc_otm,years_to_hedge,yyyymmdd_beg,yyyymmdd_end}")
    # Initialize currrent_strike, which is below the money
    current_long_price = dft.iloc[0].close
    current_strike = current_long_price * (1 - put_perc_otm)
    current_strike_array = [current_strike]

    # Create an array of high and low values, to speed up loop processing    
    m = dft[['high','low']].values

    # Loop here to determine the hedge strikes
    for i in range(1,len(m)):
        # Get high and low
        curr_high = m[i][0]
        curr_low = m[i][1]
        # If the price rises past current_strike * (1 + put_perc_otm) * (1+ put_perc_otm)
        #   then you want to roll the put strike up,essentially BUYING a put spread
        if curr_high  >= current_strike * (1 + put_perc_otm)**2:
            # roll strikes up, like buying put spreads as market goes up
            current_strike = current_strike * (1 + put_perc_otm)
        # If the price falls below current_strike * (1 - put_perc_otm) * (1- put_perc_otm)
        #   then you want to roll the put strike down, essentially SELLING a put spread
        elif curr_low <= current_strike * (1 - put_perc_otm)**2:
            # Roll strikes down (like selling put spreads as market drops)
            current_strike = current_strike * (1 - put_perc_otm)
        # Accumulate the current_strike (it either remained unchanged, went up, or went down)
        current_strike_array.append(current_strike)

    # Update dft with the current_strike array    
    dft['current_hedge_strike'] = current_strike_array
    # Create previous strike, so that you can tell when you have to buy or sell
    #   put spreads to roll your hedge to a new higher or lower level.
    dft['prev_hedge_strike'] = dft.current_hedge_strike.shift(1)
    
    # The next 2 lines are where you determine the dates on which you execute hedges
    dft.loc[dft.prev_hedge_strike!=dft.current_hedge_strike,'time_to_hedge'] = True
    dft.loc[dft.prev_hedge_strike==dft.current_hedge_strike,'time_to_hedge'] = False

    # On the next 4 rows, create the hedge_date, which will be used for calculating put prices.
    dft.loc[dft.time_to_hedge,'hedge_date'] = dft.loc[dft.time_to_hedge].settle_date
    #      Give all rows of dft that are NOT rows where time_to_hedge == True a value of the min settle_date
    dft.loc[dft.time_to_hedge==False,'hedge_date'] = dft.settle_date.min()
    #      This expanding command will make each row's hedge_date either the last hedge_date, or a new hedge_date
    dft.hedge_date = dft.hedge_date.expanding(min_periods=1).max()
    #      Now make the hedge_date a datetime object
    dft.hedge_date = dft.hedge_date.apply(yyyymmdd_to_dt)
    
    # Create days_of_hedge, which will give you the total days that the hedge was on
    dft['prev_hedge_date'] = dft.hedge_date.shift(1)
    dft['days_of_hedge'] = (dft.settle_dt - dft.hedge_date).dt.days        
    dft.loc[dft.time_to_hedge,'days_of_hedge'] = (dft[dft.time_to_hedge].hedge_date - dft[dft.time_to_hedge].prev_hedge_date).dt.days

    # Obtain atm_vol from the VIX
#     df_vix = fetch_history('^VIX',sp_data_beg_date,sp_data_end_date)
    df_vix2 = df_vix[['settle_date','close']]
    df_vix2 = df_vix2.rename(columns={'close':'atm_vol'})
    df_vix2.atm_vol = df_vix2.atm_vol / 100
    dft = dft.merge(df_vix2,on='settle_date',how='inner')

    # Obtain interest rates fro the 1 year treasury rate
    dft = dft.merge(df_1yr_rate,on='settle_date',how='inner')

    # Obtain the divident yield from the SP dividend yield dataframe
    dft['year'] = dft.settle_date.apply(lambda v:int(str(v)[0:4]))
    df_div = pd.read_csv('sp_div_yield.csv')    
    dft = dft.merge(df_div,on='year',how='inner')

    # Now calculate cost/revenue of buying put spreads, or selling put spreads
    def _calc_put_spread(r):
        return calc_put_spread(
            r.atm_vol,r.current_hedge_strike,r.prev_hedge_strike,
            r.hedge_date,r.prev_hedge_date,r.rate,put_perc_otm,years_to_hedge)
    dft.loc[dft.time_to_hedge,'hedge'] = dft.loc[dft.time_to_hedge].apply(_calc_put_spread,axis=1)
    dft.loc[dft.time_to_hedge==False,'hedge'] = 0
    dft['hedge_cumulative'] = [0] + dft.iloc[1:].hedge.cumsum().values.tolist()

    if use_fast:
        dft['hedged_value'] = np.maximum(dft.current_hedge_strike.values,dft.close.values) - dft.hedge_cumulative.values
        dft['prev_hedged_value'] = dft.hedged_value.shift(1)
        dft['hedged_daily_return'] = dft.hedged_value/dft.prev_hedged_value-1
        dft['prev_close'] = dft.close.shift(1)
        dft['unhedged_return']  = dft.close/dft.prev_close-1
    else:
        dft['hedged_value'] = dft.apply(lambda r:max(r.current_hedge_strike,r.close) - r.hedge_cumulative,axis=1)
        dft['prev_hedged_value'] = dft.hedged_value.shift(1)
        dft['hedged_daily_return'] = dft.apply(lambda r:r.hedged_value/r.prev_hedged_value-1,axis=1)
        dft['prev_close'] = dft.close.shift(1)
        dft['unhedged_return']  = dft.apply(lambda r:r.close/r.prev_close-1,axis=1) 
    
    
    # Return dft
    return dft


# #### Step 03: Define method that calculates "comparative" returns.
# 1. Return of unhedged portofolio
# 2. Return of hedged portfolio
# 3. Return of a partially invested portfolio

# In[9]:


def create_comparative_returns(dft,years_to_hedge,rebal_target,rebal_adjust,pom=.14):
    ret = {}
    
    # Get the begin and end values of dft.close, using the lowest and highest dates
    row_min = dft[dft.settle_dt == dft.settle_dt.min()].iloc[0]
    row_max = dft[dft.settle_dt == dft.settle_dt.max()].iloc[0]
    years_of_position = (row_max.settle_dt - row_min.settle_dt).days/365
    beg_value = row_min.close
    curr_value  = row_max.close
    
    # Caculate various returns
    #   return not hedged
    curr_return  = (curr_value/beg_value)**(1/years_of_position) - 1
    #   return as of the date of the highest high
    highest_high_value = dft[dft.high==dft.high.max()].iloc[0].close
    highest_return_no_hedge = (highest_high_value/beg_value)**(1/years_of_position) - 1

    #   current return if you hedged
    hedge_cost = dft[dft.time_to_hedge].hedge.sum()
    hedged_value = max(row_max.current_hedge_strike,curr_value) - hedge_cost
    hedged_return = (hedged_value/beg_value)**(1/years_of_position) - 1

    # Calculate the return from a portfolio that is rebalanced when the portfolio's
    #     percentage of stock reaches some threshold.
    
    # Get the initial shares of stock and cash
    shares = rebal_target / dft.close[0]
    cash = 1 - rebal_target
    # Set up arrays to accumlate daily changes
    cash_per_day = []
    stock_per_day = []
    port_per_day = []
    prices = dft.close.values
    dates = dft.settle_date.values
    cash_rates = dft.rate.values / 365
    rebal_dates = []
    rebal_sales = []
    stock_percs = []

    # Main loop to determine portfolio values over time, and to determine when to rebalance
    for i in range(1,len(dft)):
        # Calculate current stock dollars
        stock_dollars = shares * prices[i]
        # have your cash earn interest each day
        cash_rate = cash_rates[i]
        cash = cash * (1+cash_rate)
        # determine portfolio value 
        port = stock_dollars + cash
        # determine pre-rebalance stock percent
        stock_perc = stock_dollars/port
        stock_percs.append(stock_perc)
        # determine if you should rebalance
        if stock_perc >= rebal_adjust:
            # do upside re-balance
            dollars_to_sell = stock_dollars - rebal_target*port
            new_stock_dollars = stock_dollars - dollars_to_sell
            new_cash = cash + dollars_to_sell
            new_port = new_stock_dollars + new_cash
            shares = new_stock_dollars/prices[i]
            cash = new_cash
            stock_dollars = new_stock_dollars
            rebal_dates.append(dates[i])
            rebal_sales.append(dollars_to_sell)
        elif stock_perc <= (rebal_target - (rebal_adjust-rebal_target)):
            # do downside re-balance
            dollars_to_buy = rebal_target*port - stock_dollars
            new_stock_dollars = stock_dollars + dollars_to_buy
            new_cash = cash - dollars_to_buy
            new_port = new_stock_dollars + new_cash
            shares = new_stock_dollars/prices[i]
            cash = new_cash
            stock_dollars = new_stock_dollars
            rebal_dates.append(dates[i])
            rebal_sales.append(-dollars_to_buy)
            
        cash_per_day.append(cash)
        stock_per_day.append(stock_dollars)
        port_per_day.append(cash+stock_dollars)    
    
    df_daily_values = pd.DataFrame({
        'cash_per_day':cash_per_day,
        'stock_per_day':stock_per_day,
        'port_per_day':port_per_day,
        'close':prices[1:],
        'date':dates[1:],
        'cash_rate':cash_rates[1:],
        'stock_perc':stock_percs
    })
    df_rebalance_info = pd.DataFrame({
        'rebal_date':rebal_dates,
        'rebal_sale':rebal_sales,
    })
    # get total years and calculate annualized portfolio performance
    total_days = (dft.settle_dt.values[-1] - dft.settle_dt.values[0]).astype('timedelta64[D]')// np.timedelta64(1, 'D')
    total_years = total_days / 365
    end_port_value = port_per_day[-1]
    beg_port_value = port_per_day[0]
    annualized_port_yield = round((end_port_value/beg_port_value)**(1/total_years) - 1,3)
    return_types = [
        'total years',
        'annualized current return',
        f'annualized highest return',
        f'annualized current hedged return {round(pom*100,1)}%',
        f'rebalanced ({int(rebal_target*100)}%,{int(rebal_adjust*100)}%) portfolio end value']
    df_values = pd.DataFrame({
        'return_type':return_types,
        'current_value':[total_years,curr_value,highest_high_value,hedged_value,end_port_value],
        'return':[0,curr_return,highest_return_no_hedge,hedged_return,annualized_port_yield]})
    return df_values,df_daily_values,df_rebalance_info


# In[10]:


STYLE_TITLE={
    'line-height': '20px',
    'textAlign': 'center',
    'background-color':'#47bacc',
    'color':'#FFFFF9',
    'vertical-align':'middle',
    'horizontal-align':'middle',
} 


# #### Step 04: Create methods to convert input strings of:
# 1. beg_date in format yyyy-mm-dd (e.g. 1990-01-02 for Jan 2nd, 1990)
# 2. beg_date in format yyyy-mm-dd (e.g. 1990-01-02 for Jan 2nd, 1990)
# 3. put percent out of the money as decimal (e.g .14 for 14% out of the money)
# 
# #### into DataFrames and Graph Figures
# 

# In[11]:


def yyyymmdd_to_dt_string(yyyymmdd_int):
    y = str(yyyymmdd_int)[0:4]
    mn = str(yyyymmdd_int)[4:6]
    d = str(yyyymmdd_int)[6:8]
    return f"{y}-{mn}-{d}"

def dt_to_yyyymmdd(dt):
    yyyymmdd = int(dt.year)*100*100 + int(dt.month)*100 + int(dt.day)
    return yyyymmdd

def _get_df_values_from_input_data(input_data):
    bd = input_data[0]
    ed = input_data[1]
    perc_otm_string = input_data[2]
    rebal_target_string = input_data[3]
    rebal_adjust_string = input_data[4]
    yyyymmdd_beg = int(bd[0:4])*100*100 + int(bd[5:7])*100 + int(bd[8:10])
    yyyymmdd_end = int(ed[0:4])*100*100 + int(ed[5:7])*100 + int(ed[8:10])
    new_pom = float(perc_otm_string)
    new_rebal_target = float(rebal_target_string)
    new_rebal_adjust = float(rebal_adjust_string)
    return _get_df_values(yyyymmdd_beg,yyyymmdd_end,new_pom,new_rebal_target,new_rebal_adjust)

def _get_df_values(yyyymmdd_beg,yyyymmdd_end,
                   pom,rebal_target,rebal_adjust,years_to_hedge=1,
                   data_inputs=None):
    # validate values
    new_pom = pom
    new_rebal_target = rebal_target
    new_rebal_adjust = rebal_adjust
    dft_new = create_dft(new_pom,years_to_hedge,
                       yyyymmdd_beg=yyyymmdd_beg,yyyymmdd_end=yyyymmdd_end,
                       data_inputs=data_inputs)

    df_values,df_daily_values,df_rebalance_info = create_comparative_returns(
        dft_new,years_to_hedge,new_rebal_target,new_rebal_adjust,pom=new_pom)
    df_values.current_value = df_values.current_value.round(3) 
    df_values['return'] = df_values['return'].round(3) 
    return dft_new,df_values,df_daily_values,df_rebalance_info

def _get_graph_stock_vs_cash_figure(input_data):
    dft_new,df_values,df_daily_values,_ = _get_df_values_from_input_data(input_data)

    stock_percs = df_daily_values.stock_perc.values
    dates =  df_daily_values.date.values
    port_per_day = df_daily_values.port_per_day.values
    df_stock_perc = pd.DataFrame(
        {'dt':dates,'stock perc':stock_percs,
        '1 Dollar':port_per_day})

    annualized_port_yield = df_values['return'].values[4]    
    title = f"""Percent Stock<br>vs<br>Portfolio Value."""
    port_values_fig = plotly_plot(
        df_in=df_stock_perc,x_column='dt',yaxis2_cols=['1 Dollar'],
        plot_title=title,
        y_left_label='Percent of Portfolio in Stock',
        y_right_label='Value of 1 Portfolio Dollar',width=700,height=300,
        number_of_ticks_display=12)
    port_values_fig.update_layout(legend=dict(x=-.1, y=1.2))
    return port_values_fig


def _get_close_vs_hedge_figure(input_data):
    dft_new,_,_,_ = _get_df_values_from_input_data(input_data)
    dft_close_vs_hedge_strike = dft_new[['settle_date','close','current_hedge_strike']]
    dft_close_vs_hedge_strike = dft_close_vs_hedge_strike.rename(columns={'current_hedge_strike':'hedge strike'})
    title2 = f"""Hedge Strike<br>vs<br>SP Closing Price"""    
    fig_graph_close_vs_hedge_strike = plotly_plot(
        df_in=dft_close_vs_hedge_strike,
        x_column='settle_date',
        plot_title=title2,width=700,height=300,
        number_of_ticks_display=12,legend_x=-0.1,legend_y=1.2)
    return fig_graph_close_vs_hedge_strike

def _get_close_vs_hedge_stock_vs_cash_figure(input_data):
    dft_new,df_values,df_daily_values,_ = _get_df_values_from_input_data(input_data)
    df_daily_values['current_hedge_strike'] = dft_new.current_hedge_strike
    names = ['stock_perc','port_per_day','close','current_hedge_strike']
    x_columns = ['date' for _ in range(len(names))]
    yp_rows = [1,1,1,1]
    yp_cols = [1,1,2,2]
    yp_secondary = [False,True,False,False]
    yp_yaxis_titles = ['Stock Percent','Portolio Value','S&P Price / Hedge Strike','S&P Price / Hedge Strike']
    df_yp = pd.DataFrame({'name':names,'x_column':x_columns,
                      'row':yp_rows,'col':yp_cols,'is_secondary':yp_secondary,
                     'yaxis_title':yp_yaxis_titles})
    sp_titles = ['Stock Perc vs Portfolio Value','S&P Price vs Hedge Strike']
    fig =  dashapp.plotly_subplots(df_daily_values,df_yp,title="Portfolio Analysis",
                      num_ticks_to_display=10,subplot_titles=sp_titles) 
    fig = go.Figure(fig)
    fig.update_layout(
        legend=dict(x=-0.1, y=1.4),
        modebar={'orientation': 'v','bgcolor':'grey'}
    )
    return fig

def _get_scenarios_data(input_data):
    beg_year = int(str(input_data[0]))
    end_year = int(str(input_data[1]))
    beg_pom = float(str(input_data[2]))
    end_pom = float(str(input_data[3]))  
    all3_query = f"(year>={beg_year}) and (year<={end_year}) and (pom>={beg_pom}) and (pom<={end_pom})"
    dft_dict = build_scenarios(beg_year,end_year,beg_pom,end_pom,.6,.7)
    df_all3,df_all = build_3d_display_df(dft_dict)
    df_all3_scenarios = df_all3.query(all3_query)
    return [{'df_all3':df_all3_scenarios.to_dict('rows'),'df_all':df_all.to_dict('rows')}]
    
def _get_scenarios_figure_from_data(input_data):
    df_all3_scenarios = pd.DataFrame(input_data[0]['df_all3'])
    fig = px.scatter_3d(df_all3_scenarios, x='pom', y='year', z='ret',color='ret_type')
    fig.update_layout(
        legend=dict(x=-0.1, y=1.2),
        modebar={'orientation': 'v','bgcolor':'grey'}
    )
    return fig


    
def _get_scenarios_figure(input_data):
    beg_year = int(str(input_data[0]))
    end_year = int(str(input_data[1]))
    beg_pom = float(str(input_data[2]))
    end_pom = float(str(input_data[3]))  
    all3_query = f"(year>={beg_year}) and (year<={end_year}) and (pom>={beg_pom}) and (pom<={end_pom})"
    dft_dict = build_scenarios(beg_year,end_year,beg_pom,end_pom,.6,.7)
    df_all3,df_all = build_3d_display_df(dft_dict)
    df_all3_scenarios = df_all3.query(all3_query)
    fig = px.scatter_3d(df_all3_scenarios, x='pom', y='year', z='ret',color='ret_type')
    fig.update_layout(
        legend=dict(x=-0.1, y=1.2),
        modebar={'orientation': 'v','bgcolor':'grey'}
    )
    return fig
    
def make_page_title(title_text,div_id=None,html_container=None,parent_class=None,
                   panel_background_color='#CAE2EB'):
    par_class = parent_class
    if par_class is None:
        par_class = dashapp.pnnm
    htmc = html_container
    if htmc is None:
        htmc = html.H2
        
    title_parts = title_text.split('\n')
    

    title_list = [htmc(tp,className=dashapp.pnncnm) for tp in title_parts]
    r = dashapp.multi_row_panel(title_list,
                 parent_class=par_class,
                 div_id=div_id,
                 panel_background_color=panel_background_color) 
    return r   


# #### Step 05: Create scenarios for 3d display of returns vs year, percent out of money (pom), and rebalance percentages
# 

# In[12]:


def build_scenarios(beg_year,end_year,low_pom,high_pom,rebal_target,rebal_adjust):
    # determine yyyymmdd_end
    dt_now = datetime.datetime.now()
    yyyymmdd_end = end_year*100*100 + 1231
    yyyymmdd_now = dt_to_yyyymmdd(dt_now)
    yyyymmdd_end = min(yyyymmdd_end,yyyymmdd_now)
    # create array of beg_years to loop on
    beg_years = np.arange(beg_year,end_year,1)

    #   loop on increasing beg_year, but holding end_year constant
    dft_dict = {}
#     for y in tqdm(beg_years):
#         yyyymmdd_beg = int(y)*100*100 + 101 
#         #    loop on pom
#         for pom in [round(x,2) for x in np.arange(low_pom,high_pom+.01,.02)]:
#             dft_new,df_values,df_daily_values,df_rebalance_info =_get_df_values(
#                 yyyymmdd_beg,yyyymmdd_end,pom,rebal_target,rebal_adjust)
#             dft_dict[(y,pom)] = [dft_new,df_values,df_daily_values,df_rebalance_info] 
#     return dft_dict
    data_inputs = DataInputs()
    for y in tqdm(beg_years):
        yyyymmdd_beg = int(y)*100*100 + 101 
        #    loop on pom
        for pom in [round(x,2) for x in np.arange(low_pom,high_pom+.01,.02)]:
            dft_new,df_values,df_daily_values,df_rebalance_info =_get_df_values(
                yyyymmdd_beg,yyyymmdd_end,pom,rebal_target,rebal_adjust,
                data_inputs=data_inputs)
            dft_dict[(y,pom)] = [dft_new,df_values,df_daily_values,df_rebalance_info] 
    return dft_dict

# In[13]:


def build_3d_display_df(dft_dict):
    no_hedge_current =   [a[1].iloc[1]['return'] for a in dft_dict.values()]
    no_hedge_highest =    [a[1].iloc[2]['return'] for a in dft_dict.values()]
    with_hedge_current = [a[1].iloc[3]['return'] for a in dft_dict.values()]
    rebalanced_current =  [a[1].iloc[4]['return'] for a in dft_dict.values()]
    year_pom_array = list(dft_dict.keys())
    year_array = [a[0] for a in year_pom_array]
    pom_array  = [a[1] for a in year_pom_array]
    df_all = pd.DataFrame({
        'year':year_array,
        'pom':pom_array,
        'no_hedge_current':no_hedge_current,
        'no_hedge_highest':no_hedge_highest,
        'with_hedge_current':with_hedge_current,
        'rebalanced_current':rebalanced_current
    })

    df_all2 = df_all[['year','pom','no_hedge_current']].copy()
    df_all2 = df_all2.rename(columns={'no_hedge_current':'ret'})
    df_all2['ret_type'] = 'no_hedge_current'
    df_all2.index = list(range(len(df_all2)))
    for c in ['no_hedge_highest','with_hedge_current','rebalanced_current']:
        df_temp = df_all[['year','pom',c]].copy()
        df_temp.index=list(range(len(df_temp)))
        df_temp = df_temp.rename(columns={c:'ret'})
        df_temp['ret_type'] = c
        df_all2 = df_all2.append(df_temp,ignore_index=True)
        df_all2.index = list(range(len(df_all2)))
    df_all2.ret_type.unique()
    df_all3 = df_all2.query("ret_type in ['with_hedge_current','rebalanced_current']")
    return df_all3,df_all


# #### Step 06: Define rows of the displayed single page web app

# In[14]:


put_otm_style = {"font-size":"18px","text-align":"center","position":"relative",
    "display":"inline-block","width":"130px","height":"45px"}


# In[15]:


init_put_perc_otm=.14
init_low_pom = .10
init_high_pom = .18
init_rebal_target = .6
init_rebal_adjust = .7
init_years_to_hedge=1
init_beg_year = 1997#1990
init_beg_yyyymmdd = init_beg_year*100*100 + 701
init_end_yyyymmdd = 203001010


# In[16]:


# create row 1
def create_row_1(dap):
    # ************ Create row 1 (the main title) *******************
    app_title = """Compare Put-Protected SP500 Strategies
vs
Various Buy and Hold Strategies"""
    r1 = make_page_title(app_title,div_id='r1',html_container=html.H3)                  
    return r1


# In[17]:


# create row 2
def create_row_2(dap):
    # ************ Create comparative returns data *******************
    dft_new,df_values,df_daily_values,df_rebalance_info = _get_df_values(
#         init_beg_yyyymmdd,dt_to_yyyymmdd(datetime.datetime.now()),
        init_beg_yyyymmdd,init_end_yyyymmdd,
        init_put_perc_otm,init_rebal_target,init_rebal_adjust,
        years_to_hedge=init_years_to_hedge)
    dt_values,_ = dashapp.make_dashtable('dt_values',df_in=df_values,max_width=None)

    
    # ************ Create row 2 (the strategy results from one example run ) *********
    r2_style = {"font-size":"18px","text-align":"center"}
    dpr_beg_date =  dashapp.make_datepicker(dft_new,'beg_dp','settle_dt',style=r2_style)
    dpr_end_date =  dashapp.make_datepicker(dft_new,'end_dp','settle_dt',style=r2_style,
                                            init_date=1)
    # lambda to make dcc.Input's
    dcc_input = lambda dccid,val:dcc.Input(
        id=dccid,type="number",value=val,style=put_otm_style,debounce=True,step=.001)
        
    put_otm_inputbox = dcc_input('put_otm_inputbox',init_put_perc_otm)
    init_rebal_target_inputbox = dcc_input('init_rebal_target_inputbox',init_rebal_target)
    init_rebal_adjust_inputbox = dcc_input('init_rebal_adjust_inputbox',init_rebal_adjust)

    # row 2 column 1
    r2c1_descripts = ["begin date: ","end date: ",
                      "put% otm: ","rebal trg%: ","rebal adj%: "]
    r2c1_objs = [dpr_beg_date,dpr_end_date,put_otm_inputbox,
                 init_rebal_target_inputbox,init_rebal_adjust_inputbox]
    # lambda to make dashapp.multi_column_panel's
    r2c1_lambda = lambda i:dashapp.multi_column_panel(
        [html.Div(r2c1_descripts[i]),
        r2c1_objs[i]],grid_template=['1fr 3fr'],
        parent_class="aligncenter")
    r2c1 = dashapp.multi_row_panel([r2c1_lambda(i) for i in range(len(r2c1_objs))],
        panel_background_color='#A6B2E2',parent_class=dashapp.pn,div_id='r2c1')
    
    # row 2 col 2 row 1
    r2c2r1 = dashapp.nopanel_cell([html.H3("Compare Strategy Results")])
    # row 2 col 2 row 2
    r2c2r2 = dashapp.multi_column_panel([dt_values],
                                       parent_class=dashapp.pnncnm)
    # row 2 column 2
    r2c2 = dashapp.multi_row_panel([r2c2r1,r2c2r2],                                
                                grid_template='1fr 4fr',div_id='r2c2')
    # row 2
    r2 = dashapp.multi_column_panel(
        [r2c1,r2c2],grid_template='1fr 4fr',parent_class=dashapp.pn,div_id='r2')

    #  create a DashLink between r2c1 and the df_values DataFrame in r2c2    
    def _update_dt_values(input_data):
        dft_new,df_values,_,_ = _get_df_values_from_input_data(input_data)
        return [df_values.to_dict('rows')]
        
    r2c1_intuplist = [(dpr_beg_date,'date'),(dpr_end_date,'date'),(put_otm_inputbox,'value'),
        (init_rebal_target_inputbox,'value'),(init_rebal_adjust_inputbox,'value')]
    r2c1_outlist = [('dt_values','data')]
    r2_link = dashapp.DashLink(r2c1_intuplist,r2c1_outlist,io_callback=_update_dt_values)
    return r2,r2_link,r2c1_intuplist


# In[18]:


# create row 3
def create_row_3(dap,r2c1_intuplist):
    # *************** Create row 3 ****************************
    # row 3 subplot graphs
    get_close_vs_hedge_stock_vs_cash = dcc.Graph(
        id='get_close_vs_hedge_stock_vs_cash',style={'width':'90vw'})
    def _update_close_vs_hedge_stock_vs_cash(input_data):
        return [_get_close_vs_hedge_stock_vs_cash_figure(input_data)]
    r3_link = dashapp.DashLink(
        r2c1_intuplist,
        [(get_close_vs_hedge_stock_vs_cash,'figure')],
        io_callback=_update_close_vs_hedge_stock_vs_cash
    )
    
    r3 = dashapp.multi_column_panel([get_close_vs_hedge_stock_vs_cash],
                          parent_class=dashapp.pn,
                          div_id='r3')
    return r3,r3_link


# In[19]:


# create row 4
def create_row_4(dap):
    dft_new,df_values,df_daily_values,df_rebalance_info = _get_df_values(
        init_beg_yyyymmdd,init_end_yyyymmdd,
        init_put_perc_otm,init_rebal_target,init_rebal_adjust,
        years_to_hedge=init_years_to_hedge)
    
    dt_dft,link_for_dynamic_paging = dashapp.make_dashtable(
        'dt_dtc',df_in=dft_new,filtering=True,displayed_rows=0)
    r4 = dashapp.panel_cell([dt_dft],div_id='r4')
    return r4,link_for_dynamic_paging


# In[20]:


def create_row_5(dap):
    app_title2 = """
    Show groups of scenarios
    """
    r5 = make_page_title(app_title2,div_id='r5',html_container=html.H3)                  
    return r5


# In[21]:


def create_row_6(dap,df_all_init):
    # row 6 col 1 
#     df_spy = get_redis_df('df_spy')
#     init_beg_year = int(str(df_spy.settle_date.min())[:4])
    beg_year_inputbox = dcc.Input(
        id='beg_year_inputbox',type="number",value=int(init_beg_year),
        style=put_otm_style,debounce=True,step=1
    )    
    r6c1 = dashapp.multi_column_panel([html.Div("begin year: "),
                            beg_year_inputbox],grid_template=['1fr 3fr'],
                                        parent_class="aligncenter")
    # row 6 col 2
    init_end_year =  df_all_init.year.max()
    end_year_inputbox = dcc.Input(
        id='end_year_inputbox',type="number",value=int(init_end_year),
        style=put_otm_style,debounce=True,step=1
    )    
    r6c2 = dashapp.multi_column_panel([html.Div("end year: "),
                            end_year_inputbox],grid_template=['1fr 3fr'],
                                        parent_class="aligncenter")
    # row 6 col 3
    low_pom_inputbox = dcc.Input(
        id='low_pom_inputbox',type="number",value=init_low_pom,
        style=put_otm_style,debounce=True,step=.001
    )    
    r6c3 = dashapp.multi_column_panel([html.Div("low% otm: "),
                            low_pom_inputbox],grid_template=['1fr 3fr'],
                                        parent_class="aligncenter")
    # row 6 col 4
    high_pom_inputbox = dcc.Input(
        id='high_pom_inputbox',type="number",value=init_high_pom,
        style=put_otm_style,debounce=True,step=.001
    )    
    r6c4 = dashapp.multi_column_panel([html.Div("high% otm: "),
                            high_pom_inputbox],grid_template=['1fr 3fr'],
                                        parent_class="aligncenter")
    # row 6
    r6 = dashapp.multi_column_panel([r6c1,r6c2,r6c3,r6c4],
                          parent_class=dashapp.pn,
                          div_id='r6')
    # Create scenario inputs
    scenario_inputboxes = [beg_year_inputbox,end_year_inputbox,low_pom_inputbox,high_pom_inputbox]
    scenario_inputs = [(v,'value') for v in scenario_inputboxes]

    return r6,scenario_inputs


# In[22]:


def create_row_7(dap,scenario_inputs,df_all_init):
    # Create data store that will hold the 2 main DataFrames for row 7
    data_store = dcc.Store(id='data_store')
    data_store_loading = dcc.Loading(
        id='data_store_loading',children=[data_store],fullscreen=True)
    # the dcc.Store object must be loaded into the DOM, eventhough it is not seen
    data_store_link = dashapp.DashLink(
        scenario_inputs,[(data_store,'data')],
        io_callback=_get_scenarios_data
    )

    # Create the 3d multi scenario graph for row 7
    no_zoom_config = dict({'scrollZoom': False})
    graph_multi_scenarios = dcc.Graph(
        id='graph_multi_scenarios',config=no_zoom_config,style={'width':'45vw'})
    # Create the DashLink that links the graph to the input boxes in row 6
    def _update_multi_scenarios(input_data):
        f = _get_scenarios_figure_from_data(input_data)
        return [f]
    graph_multi_scenarios_link = dashapp.DashLink(
        [(data_store,'data')],
        [(graph_multi_scenarios,'figure')],
        io_callback=_update_multi_scenarios
    )
    
    # Create the row 7 col 2 DataFrame of data being displayed in row 7 col 1
    # Create the title of the DataFrame
    dt_multi_scenarios_title = make_page_title(
        """All Data""",
        html_container=html.H3) 
    # Create the DashLink linked the DataFrame with row 6 inputs
    dt_multi_scenarios,dt_multi_scenarios_nav_link = dashapp.make_dashtable(
        "dt_multi_scenarios",df_all_init,input_store=data_store,
        input_store_key='df_all',max_width=None)
    
    # Assemble row 7
    # row 7 col 1 
    r7c1 = dashapp.panel_cell(graph_multi_scenarios)
    # row 7 col 2 row 1 
    r7c2r1 = dt_multi_scenarios_title
    # row 7 col 2 row 2
    r7c2r2 = html.Div(dt_multi_scenarios,style={'width':'45vw'})
    # row 7 col 2
    r7c2 = dashapp.multi_row_panel([r7c2r1,r7c2r2],                                
                                grid_template='1fr 10fr',div_id='r7c2')
    # row 7
    r7 = dashapp.multi_column_panel([r7c1,r7c2],
                                    grid_template='1fr 1fr',
                                    parent_class=dashapp.pnnc,
                                    div_id='r7') 
    r7_div_list = [r7,data_store_loading]
    r7_link_list = [data_store_link,graph_multi_scenarios_link,dt_multi_scenarios_nav_link]
    return r7_div_list,r7_link_list 
    


# #### Step 07: Assemble rows and launch the app
# 1. Create an instance of DashApp, 
# 2. Create all of the html and dcc elements for each row
# 3. Create the DashLinks for each row
# 4. Call DashApp.create_app to create the main instance of Dash.app
# 

# In[23]:

if __name__=='__main__':
    panel_color = '#FFFFFA'
    all_links = []
    
    init_end_year = int(datetime.datetime.now().year)
    dft_dict_init = build_scenarios(init_end_year-1,init_end_year,init_low_pom,init_high_pom,
                               init_rebal_target,init_rebal_adjust)
    df_all3_init,df_all_init = build_3d_display_df(dft_dict_init)
    
    
    
    # Create an instance of DashApp, which holds all of the html and dcc elements, 
    #    as well as all of the DashLinks, and finally creates the instance of Dash.app
    dap = dashapp.DashApp()
    
    # *********** Assemble all of he rows and columns below ***************

    # ************ Create row 1 (the main title) *******************
    r1 = create_row_1(dap)

    # *************** Create row 2 ****************************
    r2,r2_link,r2c1_intuplist = create_row_2(dap)
    all_links.append(r2_link)
    
    # *************** Create row 3 ****************************
    r3,r3_link = create_row_3(dap,r2c1_intuplist)
    all_links.append(r3_link)
    
    # ************ Create row 4 *******************
    r4,r4_link = create_row_4(dap)
    all_links.append(r4_link)
    
    # *********** Create row 5 (title for scenario analysis in rows 6 and 7) ********************
    r5 = create_row_5(dap)
    
    # *********** Create row 6 ********************
    r6,scenario_inputs = create_row_6(dap,df_all_init)
    
    # ******** Create row 7 (holds the multi scenario output graph and DataFrame) ******
    r7_div_list,r7_link_list = create_row_7(dap,scenario_inputs,df_all_init)
    all_links = all_links + r7_link_list
    
    all_rows = html.Div([r1,r2,r3,r4,r5,r6]+r7_div_list)

    # Add all of the DashLinks to the DashApp instance (dap)
    dap.add_links(all_links)
    # Create the dash app object by calling the create_app method of dap (the instance of DashApp)
    dap.create_app(all_rows,app_title='downside_put_hedge_strategy',url_base_pathname='/dps/',app_port=8804)
    
    


# ## END

# In[ ]:





# In[ ]:




