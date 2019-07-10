'''
Created on Feb 5, 2019

Options models to create prices and greeks

@author: bperlman1
'''

import numpy as np
import pandas as pd
from scipy import stats as ss
import copy
import datetime
import pytz
import mibian

class BlackScholesBasic(object):
    '''
    Basic math to implement the Black Scholes options model
    '''
    def __init__(self, *args, **kwargs):
        object.__init__(self, *args, **kwargs)
        
    #Black and Scholes
    def d1(self,S0, K, r, sigma, T):
        if (sigma * np.sqrt(T)) ==0:
            print('zero')
        return (np.log(S0/K) + (r + sigma**2 / 2) * T)/(sigma * np.sqrt(T))
     
    def d2(self,S0, K, r, sigma, T):
        return self.d1(S0, K, r, sigma, T) - sigma * np.sqrt(T)
     
    def opt_price(self,callput,S0, K, r, sigma, T):
        if callput.lower()=="c":
            return S0 * ss.norm.cdf(self.d1(S0, K, r, sigma, T)) - K * np.exp(-r * T) * ss.norm.cdf(self.d2(S0, K, r, sigma, T))
        else:
            return K * np.exp(-r * T) * ss.norm.cdf(-self.d2(S0, K, r, sigma, T)) - S0 * ss.norm.cdf(-self.d1(S0, K, r, sigma, T))

    def delta(self,callput,S0, K, r, sigma, T):
        if callput.lower()!="c":
            return ss.norm.cdf(self.d1(S0, K, r, sigma, T))-1
        else:
            return ss.norm.cdf(self.d1(S0, K, r, sigma, T))




class BaseModel():
    '''
    Base Options model that will calculate an options price and greeks
    '''
    REFERENCE_VOL = .2
    ONE_DAY_SCAL = (1/365.0)**.5
    ATM_PERCENT_CHANGE_PER_VOL = (.001/(REFERENCE_VOL))
    VOL_PERCENT_CHANGE = .01
    INTEREST_TO_CHANGE = .001
    DEFAULT_TIMEZONE = pytz.timezone('US/Eastern')
    
    def __init__(self,expiry_datetime,strike,c_p,atm_price,input_vol,rate,carry=None,time_zone=None):
        '''
        
        :param expiry_datetime:  a python dateTime object like datetime.datetime(2019,5,22) for the March expiry of AAPL
        :param strike: any float number that represents a strike price
        :param c_p: one of "c" for call or "p" for put.  (case insensitive)
        :param atm_price: current underlying "at the money" price
        :param input_vol: annualized standard deviation of underlying
        :param rate: interest rate for the term between now and expiry
        :param carry: carry interest rate for the term between now and expiry
        :param time_zone: a pytz timezone. Default: pytz.timezine('US/Eastern')
        '''
        self.expiry_datetime = expiry_datetime
        self.strike = strike
        self.c_p = c_p
        self.atm_price = atm_price
        self.input_vol = input_vol
        self.rate = rate
        self.carry=carry
        self.time_zone= BaseModel.DEFAULT_TIMEZONE if time_zone is None else time_zone
        dt_now = datetime.datetime.now().replace(tzinfo=BaseModel.DEFAULT_TIMEZONE)
        days = (expiry_datetime - dt_now).days
        if days < 1:
            days = 1
        self.dte = days/365.0   
            
    def get_option_price(self):
        bs = BlackScholesBasic()
        op = bs.opt_price(
                          self.c_p, 
                          self.atm_price, 
                          self.strike, 
                          self.rate, 
                          self.input_vol, 
                          self.dte
        )
        return op
    
    def clone(self):
        return copy.deepcopy(self) 
    
         
    def get_delta(self):
        op = self.get_option_price()         
        
        perc_to_move =  self.input_vol *  BaseModel.ATM_PERCENT_CHANGE_PER_VOL 
        model_up = self.clone()     
        price_move =  self.atm_price * perc_to_move  
        model_up.atm_price = self.atm_price + price_move 
        op_up = model_up.get_option_price() 
        model_down = self.clone()        
        model_down.atm_price = self.atm_price - price_move 
        op_down = model_down.get_option_price() 
        d = ((op-op_down) - (op - op_up)) / (2*price_move)
        return d
    
    def get_gamma(self):
        op = self.get_delta()
        perc_to_move =  self.input_vol *  BaseModel.ATM_PERCENT_CHANGE_PER_VOL 
        model_up = self.clone()     
        price_move =  self.atm_price * perc_to_move  
        model_up.atm_price = self.atm_price + price_move 
        op_up = model_up.get_delta() 
        model_down = self.clone()        
        model_down.atm_price = self.atm_price - price_move 
        op_down = model_down.get_delta() 
        d = ((op-op_down) - (op - op_up)) / (2*price_move)
        return d
        
    def get_vega(self):
        op = self.get_option_price()         
        vol_perc_to_move = BaseModel.VOL_PERCENT_CHANGE
        model_up = self.clone()     
        model_up.input_vol =  self.input_vol + vol_perc_to_move  
        op_up = model_up.get_option_price() 
        model_down = self.clone()        
        model_down.input_vol =  self.input_vol - vol_perc_to_move  
        op_down = model_down.get_option_price() 
        v = ((op-op_down) - (op - op_up)) / (2)
        return v
        
    def get_theta(self):
        op = self.get_option_price()         
        dte_change = 1/365.0
        model_down = self.clone()        
        model_down.dte =  self.dte - dte_change  
        op_down = model_down.get_option_price() 
        t = op-op_down
        return t

    def get_rho(self):
        op = self.get_option_price()         
        int_to_move = BaseModel.INTEREST_TO_CHANGE
        model_up = self.clone()     
        model_up.rate =  self.rate + int_to_move  
        op_up = model_up.get_option_price() 
        model_down = self.clone()        
        model_down.rate =  self.rate - int_to_move  
        op_down = model_down.get_option_price() 
        r = ((op-op_down) - (op - op_up)) / (2) * .01/BaseModel.INTEREST_TO_CHANGE
        return r


class BsModel(BaseModel):
    def __init__(self,expiry_datetime,strike,c_p,atm_price,input_vol,rate,carry=None,time_zone=None):
        super(BsModel,self).__init__(expiry_datetime,strike,c_p,atm_price,input_vol,rate,carry,time_zone)
    def get_delta(self):
        bs = BlackScholesBasic()
        return bs.delta(self.c_p, self.atm_price, self.strike, self.rate, self.input_vol, self.dte)


def model_from_symbol(full_symbol,atm_price,vol=None,rate=None,carry=None,model=BaseModel,tzinfo = None):
    '''
    extract information for BaseModel constructor from a string like 'SPY_20190322_250_c'
    :param full_symbol: 
        examples:                    
            SPY_20190322_250_p   
                symbol=SPY, strike=250.0, c_p=p year=2019, month=03, day=22, hour=17, minute=00, atm=atm_price, vol=0.18, rate=0.03, carry=0.0

            SPY_201903221230_250_c  
                symbol=SPY strike=250.0, c_p=c, year=2019, month=03, day=22, hour=12, minute=30, atm=atm_price, vol=0.18, rate=0.03, carry=0.0

            SPY_20190322_250 
                symbol=SPY strike=250.0, c_p=c, year=2019, month=03, day=22, hour=17, minute=00, atm=atm_price, vol=0.18, rate=0.03, carry=0.0

            SPY_20190322_250_c_v.32_r.05_c.01 to specify any of  vol, rate or carry
                symbol=SPY strike=250.0, c_p=c, year=2019, month=03, day=22, hour=17, minute=00, atm=atm_price, vol=0.32, rate=0.05, carry=0.01                

            SPY  for the underlying, which is call option with a strike of .00001
                If today is March 20th 2019, then:
                symbol=SPY strike=.0001, c_p=p year=2019, month=03, day=21, hour=17, minute=00, atm=atm_price, vol=0.18, rate=0.03, carry=0.0
                

    :param tzinfo:
    '''
    tz = tzinfo if tzinfo is not None else BaseModel.DEFAULT_TIMEZONE
    parts = full_symbol.split('_')
    lp = len(parts)
    expiry_date = datetime.datetime.now().replace(tzinfo=tz) + datetime.timedelta(5)
    if lp>1:
        year = int(parts[1][0:4])
        mon = int(parts[1][4:6])
        day = int(parts[1][6:8])
        hour = 17 if len(parts[1])<10 else int(parts[1][8:10])
        minute = 0 if len(parts[1])<10 else int(parts[1][10:12])
        expiry_date = datetime.datetime(year,mon,day,hour,minute,tzinfo=tz)
    strike  = .00001
    if lp>2:
        strike = float(str(parts[2]))
    c_p  = 'c'
    if lp > 3:
        c_p = 'c' if lp < 3 else parts[3]
    
    param_dict = {
        'v':.18 if vol is None else vol,
        'r':.03 if rate is None else rate,
        'c':0 if carry is None else carry,
        }
    for i in range(4,8):
        if lp>i:
            key = parts[i][0]
            value = parts[i][1:]
            param_dict[key] = value
    vol = float(param_dict['v'])
    r = float(param_dict['r'])
    carry = float(param_dict['c'])
    
    op = model(expiry_date,strike,c_p,atm_price,vol,r,carry)
    return op

def symbol_from_model(symbol,model=BaseModel):
    atm = model.atm_price
    dt = model.expiry_datetime
    year = dt.year
    mon = dt.month 
    day = dt.day 
    hour = dt.hour 
    minute = dt.minute 
    yyyymmddhhmm = str('%04d%02d%02d%02d%02d' %(year,mon,day,hour,minute))
    strike = str(model.strike)
    c_p = model.c_p 
    vol = str(model.input_vol)
    r = str(model.rate)
    c = str(model.carry)
    return f'{symbol}_{yyyymmddhhmm}_{strike}_{c_p}_v{vol}_r{r}_c{c}'

def yyyymmddhhmm_from_datetime(dt): 
    year = dt.year
    mon = dt.month 
    day = dt.day 
    hour = dt.hour 
    minute = dt.minute 
    yyyymmddhhmm = str('%04d%02d%02d%02d%02d' %(year,mon,day,hour,minute))
    return yyyymmddhhmm

def get_greeks(symbol,atm_price,op_model=None):
    '''
    Create pandas DataTable where columns are underlying, yyyymmddhhmm_expiry strike, delta, gamma, vega, theta, rho, and implied_vol
    :param df_portfolio:
    '''
    m = model_from_symbol(symbol,atm_price,model=op_model)
    p = m.get_option_price()
    d = m.get_delta()
    g = m.get_gamma()
    v = m.get_vega()
    t = m.get_theta()
    rh = m.get_rho()
    underlying = symbol.split('_')[0]
    return {
        'symbol':symbol,
        'underlying':underlying,
        'option_price':p,
        'delta':d,
        'gamma':g, 
        'vega':v,
        'theta':t,
        'rho':rh
        }
def get_df_greeks(df_portfolio,df_atm_price,model_per_underlying_dict):
    dfp = df_portfolio.copy()
    if 'underlying' not in dfp.columns.values:
        dfp['underlying'] = dfp.apply(lambda r: r.symbol.split('_')[0],axis=1)
        
    df_all = dfp.merge(df_atm_price,how='inner',on='underlying')
    greek_cols = ['delta','gamma','vega','theta','rho']
    def _greeks(row):
        symbol = row.symbol
        atm_price =row.price 
        size = row.position
        op_model = model_per_underlying_dict[row.underlying]
        greeks = get_greeks(symbol, atm_price, op_model)
        for g in greek_cols:
            greeks[g] = greeks[g] * float(size)
        greeks['symbol'] = row.symbol
        greeks['underlying'] = row.underlying
        greeks['position'] = row.position
        s = pd.Series(greeks)
        return s
    df_greeks = df_all.apply(_greeks,axis=1)
    df_greeks = df_greeks[['symbol','underlying','position','option_price','delta','gamma','vega','theta','rho']]
    cols_no_symbol =[c for c in df_greeks.columns.values if c not in ['symbol','option_price','position']]
    df_greeks_totals = df_greeks[cols_no_symbol].groupby('underlying',as_index=False).sum()    
    return {'df_greeks':df_greeks,'df_greeks_totals':df_greeks_totals}
 
if __name__=='__main__':
    pc = 'c'
    s = 100
    k = 100
    vol = .2
    r = .03
    car = 0
    expiry_date = datetime.datetime.now().replace(tzinfo=BaseModel.DEFAULT_TIMEZONE) + datetime.timedelta(30)
    m = BaseModel(expiry_date,s,pc,k,vol,r,car)
    p = m.get_option_price()
    d = m.get_delta()
    g = m.get_gamma()
    v = m.get_vega()
    t = m.get_theta()
    rh = m.get_rho()
    print('basic model call price',p,d,g,v,t,rh)
    
    m = BaseModel(expiry_date,s,pc,k,vol,r-.01,car)
    p = m.get_option_price()
    print('price with rate - .01',p)
    m = BaseModel(expiry_date,s,pc,k,vol,r+.01,car)
    p = m.get_option_price()
    print('price with rate + .01',p)
    
    sym = f'TEST_{yyyymmddhhmm_from_datetime(expiry_date)}_{k}_{pc}_v{vol}_r{r}_c{car}'
    m = model_from_symbol(sym,100)
    p = m.get_option_price()
    d = m.get_delta()
    g = m.get_gamma()
    v = m.get_vega()
    t = m.get_theta()
    rh = m.get_rho()
    print(f'basic model call price using model_from_symbol({sym})',p,d,g,v,t,rh)
    
    sym = f'TEST_{yyyymmddhhmm_from_datetime(expiry_date)}'
    m = model_from_symbol(sym,100)
    p = m.get_option_price()
    d = m.get_delta()
    g = m.get_gamma()
    v = m.get_vega()
    t = m.get_theta()
    rh = m.get_rho()
    print(f'basic model call price using model_from_symbol({sym})',p,d,g,v,t,rh)
    
    sym = 'TEST'
    m = model_from_symbol(sym,100)
    p = m.get_option_price()
    d = m.get_delta()
    g = m.get_gamma()
    v = m.get_vega()
    t = m.get_theta()
    rh = m.get_rho()
    print(f'basic model call price using model_from_symbol({sym})',p,d,g,v,t,rh)
    
    yyyymmddhhmm = yyyymmddhhmm_from_datetime(expiry_date)
    op_syms = [f'SPY_{yyyymmddhhmm}_255_c',f'SPY_{yyyymmddhhmm}_255_p',
               f'SPY_{yyyymmddhhmm}_260_c',f'SPY_{yyyymmddhhmm}_260_p',
               f'SPY_{yyyymmddhhmm}_265_c',f'SPY_{yyyymmddhhmm}_265_p',
               f'USO_{yyyymmddhhmm}_10_c',f'USO_{yyyymmddhhmm}_10_p',
               f'USO_{yyyymmddhhmm}_10.5_c',f'USO_{yyyymmddhhmm}_10.5_p',
               f'USO_{yyyymmddhhmm}_11_c',f'USO_{yyyymmddhhmm}_11_p',
               ]
    df_portfolio = pd.DataFrame({'symbol':op_syms,'position':[10,-10,10,-10,10,-10,10,-10,10,-10,10,-10]})
    
    df_atm_price = pd.DataFrame({'underlying':['SPY','USO'],'price':[260,10.5]})
    model_per_underlying_dict = {'SPY':BsModel,'USO':BsModel}
    greeks_dict = get_df_greeks(df_portfolio, df_atm_price, model_per_underlying_dict)
    print(greeks_dict['df_greeks'])
    print(greeks_dict['df_greeks_totals'])
    
        