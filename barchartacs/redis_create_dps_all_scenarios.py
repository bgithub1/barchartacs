#!/usr/bin/env python
# coding: utf-8

# ## Create DataFrame for all scenarios of the DPS strategy
# DPS = Downside Put Strategy
# 

# ## Create a hedge strategy and use data on ^GSPC from yahoo 

# ### Calculate the cost/revenue of the hedge.
# The put hedge that you will buy will initially be below the current SP price by a percentage which you set in the variable ```put_perc_otm```.  When the price of the SP rises high enough so that you can raise the strike price of the hedge, you sell the current put (if there is any value in it) and buy a new put that is ```put_perc_otm``` percent higher than the previous put.  In this way, you are not letting your hedge get too far from the money.
# 
# 
# * Remember that, since you are comparing this put strategy to "Buy-And-Hold"
#   * Rolls to a higher strike are a cost to the strategy
#   * Rolls to a lower strike are revenue to the strategy.

# In[1]:


import pandas as pd
import numpy as np

import sys
import os
if  not os.path.abspath('./') in sys.path:
    sys.path.append(os.path.abspath('./'))
if  not os.path.abspath('../') in sys.path:
    sys.path.append(os.path.abspath('../'))


import datetime
# from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from py_vollib import black
import pdb

import pyarrow as pa
import redis
import time
import schedule_it#@UnresolvedImport

redis_port = 6379
redis_db = redis.Redis(host = 'localhost',port=6379,db=0)



def get_redis_df(key):
    context = pa.default_serialization_context()#@UndefinedVariable
    df = context.deserialize(redis_db.get(key))
    return df


def update_redis_df(key,df):
    context = pa.default_serialization_context()#@UndefinedVariable
    redis_db.set(key, context.serialize(df).to_buffer().to_pybytes())


def schedule_updates(hour,update_callback):
    logger = schedule_it.init_root_logger("logfile.log", "INFO")
    while True:
        logger.info(f"scheduling update for hour {hour}")
        sch = schedule_it.ScheduleNext('hour', hour,logger = logger)
        sch.wait()
        logger.info(f"updating history")
        update_db()
        logger.info(f"sleeping for an hour before next scheduling")
        time.sleep(60*60)
    


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


# In[8]:


def yyyymmdd_to_dt_string(yyyymmdd_int):
    y = str(yyyymmdd_int)[0:4]
    mn = str(yyyymmdd_int)[4:6]
    d = str(yyyymmdd_int)[6:8]
    return f"{y}-{mn}-{d}"




# #### Step 02: Define methods that creates the dataframe called ```dft``` which has all of the strategy info, incluing hedge values.
# 
# 

# In[9]:


class DataInputs():
    def __init__(self):
        self.df_spy = get_redis_df('df_spy')
        self.df_vix = get_redis_df('df_vix')
        self.df_1yr_rate = get_redis_df('df_1yr_rate')
        self.df_div = get_redis_df('df_div')
        


# In[10]:


def create_dft(put_perc_otm,years_to_hedge,
              yyyymmdd_beg=None,yyyymmdd_end=None,use_fast=True,
              data_inputs=None):
    if data_inputs is None:
        df_spy = get_redis_df('df_spy')
        df_vix = get_redis_df('df_vix')
        df_1yr_rate = get_redis_df('df_1yr_rate')
    else:
        df_spy = data_inputs.df_spy
        df_vix = data_inputs.df_vix
        df_1yr_rate = data_inputs.df_1yr_rate
        
            
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

# In[11]:


def create_comparative_returns(dft,years_to_hedge,rebal_target,rebal_adjust,pom=.14):
    
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
#             new_port = new_stock_dollars + new_cash
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
#             new_port = new_stock_dollars + new_cash
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


def _get_df_values(yyyymmdd_beg,yyyymmdd_end,pom,rebal_target,rebal_adjust,
                   years_to_hedge=1,data_inputs=None):
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



def build_all_scenarios(beg_year,end_year,low_pom,high_pom,rebal_target,rebal_adjust):
    # determine yyyymmdd_end
    dt_now = datetime.datetime.now()
    yyyymmdd_end = end_year*100*100 + 1231
    yyyymmdd_now = dt_to_yyyymmdd(dt_now)
    yyyymmdd_end = min(yyyymmdd_end,yyyymmdd_now)
    # create array of beg_years to loop on
    beg_years = np.arange(beg_year,end_year,1)

    #   loop on increasing beg_year, but holding end_year constant
    data_inputs = DataInputs()
    year_begs = []
    poms = []
    no_hedge_currents = []
    no_hedge_highests = []
    with_hedge_currents = []
    rebalanced_currents = []
    for y in tqdm(beg_years):
        yyyymmdd_beg = int(y)*100*100 + 101 
        #    loop on pom
        for pom in [round(x,2) for x in np.arange(low_pom,high_pom+.01,.02)]:
            _,df_values,_,_ =_get_df_values(
                yyyymmdd_beg,yyyymmdd_end,pom,rebal_target,rebal_adjust,
                data_inputs=data_inputs)
            year_begs.append(y)
            poms.append(pom)
            no_hedge_currents.append(df_values.iloc[1]['return'])
            no_hedge_highests.append(df_values.iloc[2]['return'])
            with_hedge_currents.append(df_values.iloc[3]['return'])
            rebalanced_currents.append(df_values.iloc[4]['return'])
    return pd.DataFrame(
        {'year_beg':year_begs,'pom':poms,'no_hedge_current':no_hedge_currents,
         'no_hedge_highest':no_hedge_highests,'with_hedge_current':with_hedge_currents,
         'rebalanced_current':rebalanced_currents})




def update_db():
    df_every_scenario = None
    for y in tqdm(range(2000,2021,1)):
        df_1 = build_all_scenarios(1990,y,.1,.18,.6,.7)
        df_1['year_end'] = y
        if df_every_scenario is None:
            df_every_scenario = df_1.copy()
        else:
            df_every_scenario = df_every_scenario.append(df_1,ignore_index=True)
        df_every_scenario.index = list(range(len(df_every_scenario)))    
    update_redis_df('df_every_scenario',df_every_scenario)    



if __name__=='__main__':
    h = 19 if len(sys.argv)<2 else int(sys.argv[1])
#     update_db()
    schedule_updates(h,update_db)


