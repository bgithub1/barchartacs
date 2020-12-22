#!/usr/bin/env python
# coding: utf-8

# In[51]:


import pandas as pd
import sys,os
this_dir = os.path.abspath('.')
parent_dir = os.path.abspath('..')
sys.path.append(parent_dir)
sys.path.append(this_dir)
import db_info#@UnresolvedImport

import dash
import dash_html_components as html
from flask import Flask,make_response,request as flreq
from flask_restful import Resource, Api
import typing

opttab = 'sec_schema.options_table'
futtab = 'sec_schema.underlying_table'

StrList = typing.List[str]
class SqlDownloader():
    def __init__(self,file_name:str,config_name:str):
        self.file_name = file_name
        self.pga = db_info.get_db_info(config_name=config_name)


    def get_options(self,symbol:str,yyyymmdd_beg:int=None,yyyymmdd_end:int=None):
        sql = f"select * from {opttab} where symbol='{symbol}'"
        if yyyymmdd_beg is not None:
            sql += f' and settle_date>={yyyymmdd_beg}'
        if yyyymmdd_end is not None:
            sql += f' and settle_date<={yyyymmdd_end}'
        df =  self.pga.get_sql(sql)
        if df is not None and len(df)>2:
            df = df.sort_values(['pc','settle_date','strike'])
        return df

    def get_futures(self,symbol_list:StrList,yyyymmdd_beg:int=None,yyyymmdd_end:int=None):
        sym_string = ",".join([f"'{s}'" for s in symbol_list])        
        sql = f"select * from {futtab} where symbol in ({sym_string})"
        if yyyymmdd_beg is not None:
            sql += f' and settle_date>={yyyymmdd_beg}'
        if yyyymmdd_end is not None:
            sql += f' and settle_date<={yyyymmdd_end}'
        df =  self.pga.get_sql(sql)
        if df is not None and len(df)>2:
            df = df.sort_values(['symbol','settle_date'])
        return df
    
    def create_options_downloader(self): 
        parent_self = self
        class SecDbOptions(Resource):
            def get(self):
                df = pd.DataFrame({'x':[1,2,3,4,5],'y':[21,22,23,24,25]})
                symbol = flreq.args.get('symbol')
                fn = f"{parent_self.file_name}_{symbol}"
                yyyymmdd_beg = flreq.args.get('yyyymmddbeg')
                yyyymmdd_end = flreq.args.get('yyyymmddend')
                if yyyymmdd_beg is not None:
                    yyyymmdd_beg = int(str(yyyymmdd_beg))
                    fn = f"{fn}_{yyyymmdd_beg}"
                if yyyymmdd_end is not None:
                    yyyymmdd_end = int(str(yyyymmdd_end))
                    fn = f"{fn}_{yyyymmdd_end}"
                df = parent_self.get_options(symbol,yyyymmdd_beg,yyyymmdd_end)
                resp = make_response(df.to_csv(index=False))
                resp.headers["Content-Disposition"] = f"attachment; filename={fn}.csv"
                resp.headers["Content-Type"] = "text/csv"
                return resp        
        return SecDbOptions

    def create_futures_downloader(self): 
        parent_self = self
        class SecDbFutures(Resource):
            def get(self):
                df = pd.DataFrame({'x':[1,2,3,4,5],'y':[21,22,23,24,25]})
                symbol_list = str(flreq.args.get('symbol')).split(',')
                symlist_string = '_'.join(symbol_list)
                fn = f"{parent_self.file_name}_{symlist_string}"
                yyyymmdd_beg = flreq.args.get('yyyymmddbeg')
                yyyymmdd_end = flreq.args.get('yyyymmddend')
                if yyyymmdd_beg is not None:
                    yyyymmdd_beg = int(str(yyyymmdd_beg))
                    fn = f"{fn}_{yyyymmdd_beg}"
                if yyyymmdd_end is not None:
                    yyyymmdd_end = int(str(yyyymmdd_end))
                    fn = f"{fn}_{yyyymmdd_end}"
                df = parent_self.get_futures(symbol_list,yyyymmdd_beg,yyyymmdd_end)
                resp = make_response(df.to_csv(index=False))
                resp.headers["Content-Disposition"] = f"attachment; filename={fn}.csv"
                resp.headers["Content-Type"] = "text/csv"
                return resp        
        return SecDbFutures
    
if __name__=='__main__':
    # argv[1] = port, argv[2] = config_name of db
    # get pga
#     app_port = 8814
#     config_name = 'local'
    app_host = '127.0.0.1'
    argvs = sys.argv
    app_port = int(argvs[1])
    config_name = argvs[2]
    server = Flask('sec_db')
    url_base_pathname=f'/app{app_port}/'
    app = dash.Dash(server=server,prevent_initial_callbacks=True,url_base_pathname=url_base_pathname)
    api = Api(server)
    sqld = SqlDownloader('mycsv',config_name)
    api.add_resource(sqld.create_options_downloader(), f'/app{app_port}/options')
    api.add_resource(sqld.create_futures_downloader(), f'/app{app_port}/futures')
    app.layout = html.Div([])
    print(f"Running server on {app_host}:{app_port}/app{app_port}")
    print(f"For options: http://{app_host}:{app_port}/app{app_port}/options?symbol=CLG21&yyyymmddbeg=20200801&yyyymmddend=20201201")
    print(f"For futures: http://{app_host}:{app_port}/app{app_port}/futures?symbol=CLG21&yyyymmddbeg=20200801&yyyymmddend=20201201")
    app.run_server(host=app_host,port=app_port)



