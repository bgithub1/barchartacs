import os,sys
import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay
import datetime
import pytz
from dateutil.relativedelta import *
import pandas_market_calendars as pmc
from dashapp import dashapp2 as dashapp
import re
from pandas.tseries.holiday import USFederalHolidayCalendar
import db_info
import pdb
from tqdm import tqdm,tqdm_notebook

# important constants used below
MONTH_CODES = 'FGHJKMNQUVXZ'
DICT_MONTH_CODE = {MONTH_CODES[i]:i+1 for i in range(len(MONTH_CODES))}
TIMEZONE = 'US/Eastern'
CHROME_DOWNLOADED=False
THIS_DECADE = int(str(int(datetime.datetime.now().year))[2])*10
DO_DATABASE_COMPARE=False

from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, \
    next_monday,next_monday_or_tuesday,USMartinLutherKingJr, USPresidentsDay, \
    GoodFriday, USMemorialDay, USLaborDay, USThanksgivingDay,EasterMonday

class USTradingCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday),
    ]


class UKTradingCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        GoodFriday,
        EasterMonday,
        Holiday('EarlyMayHoliday',month=5,day=1,offset=pd.DateOffset(weekday=MO(1))),
        Holiday('SpringBankHoliday',month=6,day=1,offset=pd.DateOffset(weekday=MO(-1))),
        Holiday('SummerBankHoliday',month=9,day=1,offset=pd.DateOffset(weekday=MO(-1))),
        Holiday('StAndrewsDay',month=11,day=30),
        Holiday('Christmas', month=12, day=25, observance=next_monday),
        Holiday('Christmas', month=12, day=26, observance=next_monday_or_tuesday),
    ]    


# define business day calenders to use below    
bday_us = pd.offsets.CustomBusinessDay(calendar=USTradingCalendar())    
bday_uk = pd.offsets.CustomBusinessDay(calendar=UKTradingCalendar())


def get_thanksgiving_dt(year):    
    iyyyy = int(year)
    holidays = USTradingCalendar().holidays(start=f'{iyyyy}-11-01', end=f'{iyyyy}-11-30', return_name=True)
    df_holidays = holidays.to_frame()
    df_holidays =df_holidays.reset_index()
    df_holidays.columns=['hdate','hname']
    return df_holidays[['giving' in v.lower() for v  in df_holidays.hname.values]].hdate.values[0]



_DAY_INDEX = ['MON','TUE','WED','THU','FRI','SAT','SUN']
def get_nth_weekday(year,month,target_weekday,nth_occurrence):
    '''
    weekday is the term that assigns numbers from 0 to 6 to the days of the weeks.
    weekday 0 = monday
    
    Example:
    n = datetime.datetime.now()
    year_now = int(n.year)
    month_now = int(n.month)
    [get_nth_weekday(year_now,m+1,3,3) for m in range(month_now,12)]  
    
    '''
    f = f'W-{_DAY_INDEX[target_weekday]}'
    dr = pd.date_range(datetime.datetime(year,month,1), periods=nth_occurrence, freq=f)[-1]
    return dr




def get_ES_options_expiry(symbol):
    '''
    3rd friday of month of symbol
    '''
    monthcode_yy = symbol[2:]
    month = DICT_MONTH_CODE[monthcode_yy[0]]
    year = 2000 + int(monthcode_yy[1:])
    return get_nth_weekday(year,month,4,3)


def get_E6_options_expiry(symbol):
    monthcode_yy = symbol[2:]
    next_month = DICT_MONTH_CODE[monthcode_yy[0]] + 1
    year = 2000 + int(monthcode_yy[1:])
    if next_month>12:
        next_month = 1
        year += 1
    return datetime.datetime(year,next_month,1) - 7*bday_us

def get_LO_options_expiry(symbol):
    return get_CL_options_expiry('CL'+symbol[-3:])

def get_CL_options_expiry(symbol):
    '''
    Trading terminates 7 business days before the 26th calendar of the month prior to the contract month.
    '''
    monthcode_yy = symbol[2:]
    month = DICT_MONTH_CODE[monthcode_yy[0]]
    year = 2000 + int(monthcode_yy[1:])
    month = month -1
    if month<1:
        month = 12
        year = year - 1
    return datetime.datetime(year,month,26) - 7*bday_us

def get_CL_futures_expiry(symbol):
    '''
    Trading terminates 3 business days before the 25th calendar of the month prior to the contract month.
    '''
    monthcode_yy = symbol[2:]
    month = DICT_MONTH_CODE[monthcode_yy[0]]
    year = 2000 + int(monthcode_yy[1:])
    month = month -1
    if month<1:
        month = 12
        year = year - 1
    return datetime.datetime(year,month,25) - 3*bday_us

def get_ON_options_expiry(symbol):
    return get_NG_options_expiry('NG'+symbol[-3:])

def get_LNE_options_expiry(symbol):
    return get_NG_options_expiry('NG'+symbol[-3:])

def get_NG_options_expiry(symbol):
    monthcode_yy = symbol[-3:]
    month = DICT_MONTH_CODE[monthcode_yy[0]]
    year = 2000 + int(monthcode_yy[1:])
    return datetime.datetime(year,month,1) - 4*bday_us

def get_NG_futures_expiry(symbol):
    # Trading terminates on the 3rd last business day of the month prior to the contract month
    monthcode_yy = symbol[-3:]
    month = DICT_MONTH_CODE[monthcode_yy[0]]
    year = 2000 + int(monthcode_yy[1:])
    return datetime.datetime(year,month,1) - 3*bday_us


def get_GE_options_expiry(symbol):
    """
    Quarterly:
    The second London bank business day before the third Wednesday 
    of the contract month. Trading in expiring contracts ceases at 
    11:00 a.m. London Time on the last trading day
    
    Serialy:
    The Friday immediately preceding the third Wednesday of the contract month.
    """
    monthcode_yy = symbol[-3:]
    month = DICT_MONTH_CODE[monthcode_yy[0]]
    year = 2000 + int(monthcode_yy[1:])
    wed_3 = get_nth_weekday(year,month,2,3)
    mc = monthcode_yy[0]
    dr = pd.date_range(datetime.datetime(year,month,1), periods=3, freq='W-WED')
    if mc in ['H','M','U','Z']:        
        r = [d - 2*bday_uk for d in dr][-1]
    else:
        r = [d - datetime.timedelta(5) for d in dr][-1] + 1*bday_uk - 1*bday_uk
    return r

def get_CB_options_expiry(symbol):
    '''
    This is the spec for the CME Brent, but it matches ICE.
    Trading terminates the 4th last London business day of 
    the month, 2 months prior to the contract month 
    except for the February contract month which 
    terminates the 5th last London business day of the 
    month, 2 months prior to the contract month.  
    '''
    monthcode_yy = symbol[2:]
    month = DICT_MONTH_CODE[monthcode_yy[0]]
    year = 2000 + int(monthcode_yy[1:])
    month = month - 1
    if month<1:
        month = 12 + month
        year = year - 1
    days_to_subtract = 4
    if monthcode_yy[0] =='G':
        days_to_subtract = 5
    elif monthcode_yy[0] == 'F':
        days_to_subtract = 3
#     elif monthcode_yy == 'N22':
#         days_to_subtract = 7
    return datetime.datetime(year,month,1,0,0) - days_to_subtract * bday_uk


def get_GC_options_expiry(symbol):
    """
    Trading terminates at 12:30 p.m. CT on the 4th last business day of the month
    prior to the contract month. 
    If the 4th last business day occurs on a Friday 
    or the day before a holiday, 
    trading terminates on the prior business day.
    """
    exp_dt = get_ON_options_expiry(symbol)
    r = exp_dt
    if exp_dt.weekday()==4:
        r =  exp_dt - 1*bday_us
    td1 = datetime.timedelta(1)
    is_day_before_thanksgiv = (get_thanksgiving_dt(r.year) - (r + td1)).days == 0 
    if is_day_before_thanksgiv:
        r = r - td1
    return r

def get_ZS_options_expiry(symbol):
    """
    Trading terminates on Friday which precedes, by at least 2 business days, 
    the last business day of the month prior to the contract month.
    """
    monthcode_yy = symbol[-3:]
    month = DICT_MONTH_CODE[monthcode_yy[0]]
    year = 2000 + int(monthcode_yy[1:])
    r =  datetime.datetime(year,month,1) - 3*bday_us
#     pdb.set_trace()
    dow = r.weekday()
    if dow>=4:
        days_to_subtract = dow-4
    else:
        # find the first friday before r
        days_to_subtract = dow-4+7
    r = r - datetime.timedelta(days_to_subtract)
    return r+1*bday_us - 1*bday_us
        

DICT_PRODUCT = {
    'E6':get_E6_options_expiry,
    'ES':get_ES_options_expiry,
    'CL':get_CL_options_expiry,
    'NG':get_NG_options_expiry,
    'LO':get_LO_options_expiry,
    'ON':get_ON_options_expiry,
    'LNE':get_LNE_options_expiry,
    'LN':get_ON_options_expiry,
    'CB':get_CB_options_expiry,
    'RB':get_ON_options_expiry,
    'HO':get_ON_options_expiry,
    'OB':get_ON_options_expiry,
    'OH':get_ON_options_expiry,
    'GC':get_GC_options_expiry,
    'OG':get_GC_options_expiry,
    'SI':get_GC_options_expiry,
    'SO':get_GC_options_expiry,
    'ZS':get_ZS_options_expiry,
    'OZS':get_ZS_options_expiry,
    'ZC':get_ZS_options_expiry,
    'OZC':get_ZS_options_expiry,
    'ZW':get_ZS_options_expiry,
    'OZW':get_ZS_options_expiry,
    'ZL':get_ZS_options_expiry,
    'OZL':get_ZS_options_expiry,
    'GE':get_GE_options_expiry,
}

DICT_FUTURES_PRODUCT = {
    'CL':get_CL_futures_expiry,
    'ES':get_ES_options_expiry, # futures and options expire together,
    'NG':get_NG_futures_expiry
}

    
def get_expiry(symbol,is_option=True):
    '''
    Example:
    get_expiry('GEZ20')
    '''
    product = symbol[:-3]
    if is_option:
        f = DICT_PRODUCT[product]
    else:
        f = DICT_FUTURES_PRODUCT[product]
        
    expiry =  f(symbol)
    if (expiry.weekday()==0) and (expiry.month==12 and expiry.day==27):
        expiry=expiry + 1*bday_us
    return expiry


def dt_from_yyyymmdd(yyyymmdd,hour=0,minute=0,timezone=TIMEZONE):
    y = int(str(yyyymmdd)[0:4])
    m = int(str(yyyymmdd)[4:6])
    d = int(str(yyyymmdd)[6:8])  
    return datetime.datetime(y,m,d,hour,minute,tzinfo=pytz.timezone(timezone))

def yyyymmdd_from_dt(dt):
    y = int(dt.year)
    m = int(dt.month)
    d = int(dt.day)
    return y*100*100 + m*100 + d

def get_dte(trade_yyyymmdd,expiry_yyyymmdd):
    dt_td = dt_from_yyyymmdd(trade_yyyymmdd)
    dt_xp = dt_from_yyyymmdd(expiry_yyyymmdd)
    return (dt_xp - dt_td).days


def get_dte_pct(trade_yyyymmdd,expiry_yyyymmdd):
    dt_td = dt_from_yyyymmdd(trade_yyyymmdd)
    dt_xp = dt_from_yyyymmdd(expiry_yyyymmdd)
    return ((dt_xp - dt_td).days + 1)/365


def get_full_underlying_symbol(r,decade=2,underlying_col='Underlying Symbol'):
    underyear = int(re.findall('[0-9]{1,2}$',r[underlying_col])[0])
    if underyear > 9:
        return r[underlying_col]
    under_symbol_no_year = r[underlying_col][:-1]
    return under_symbol_no_year + str(decade*10+underyear)

def get_full_option_symbol(r,decade=2,option_col='Option Symbol',
                           underlying_col='Underlying Symbol'):
    optyear = int(re.findall('[0-9]{1,2}$',r[option_col])[0])
    if optyear>9:
        return r[option_col]

    # we have a one character option year
    # first see if this option month is different than the underlying month
    op_monthcode = re.findall('[A-Z][0-9]{1,2}$',r[option_col])[0][0]
    un_monthcode = re.findall('[A-Z][0-9]{1,2}$',r[underlying_col])[0][0]
    underyear = int(re.findall('[0-9]{1,2}$',r[underlying_col])[0])
    if (underyear > 9) and (op_monthcode==un_monthcode):
        return r[option_col][:-1] + str(underyear)
    opt_symbol_no_year = r[option_col][:-1]
    return opt_symbol_no_year + str(decade*10+optyear)


def get_cme_expiries():
    '''
    Scrape the CME website to get current expiries for many futures contracts
    '''
    df_eee_all = None
    for i in tqdm(range(1,7)):
        url = f'https://cmegroup-tools.quikstrike.net/User/Export/CME/ExpirationCalendar.aspx?GroupId={i}'
        df_eee_temp = pd.read_csv(url)
        if df_eee_all is None:
            df_eee_all = df_eee_temp.copy()
        else:
            df_eee_all = df_eee_all.append(df_eee_temp,ignore_index=True)
        df_eee_all.index = list(range(len(df_eee_all)))

    oexps = [s[:10] for s in df_eee_all['Option Expiration Date (CT)'].values]
    exp_dts = [datetime.datetime.strptime(str_date,'%m/%d/%Y') for str_date in oexps]
    df_eee_all['option_expiry'] = exp_dts

    unexps = ['01/01/1900' if 'nan' in str(s).lower() else s[:10] for s in df_eee_all['Underlying Expiration Date (CT)'].values]
    unexp_dts = [datetime.datetime.strptime(str_date,'%m/%d/%Y') for str_date in unexps]
    df_eee_all['underlying_expiry'] = unexp_dts

    df_eee_all['underlying_symbol'] = df_eee_all.apply(get_full_underlying_symbol,axis=1)
    opsym_lambda = lambda r:get_full_option_symbol(r,underlying_col='underlying_symbol')
    df_eee_all['option_symbol'] = df_eee_all.apply(opsym_lambda,axis=1)
    return df_eee_all

    

