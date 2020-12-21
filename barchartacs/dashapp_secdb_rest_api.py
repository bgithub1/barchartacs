
import numpy as np
import pandas as pd
import sys,os
this_dir = os.path.abspath('.')
parent_dir = os.path.abspath('..')
sys.path.append(parent_dir)
sys.path.append(this_dir)
print(sys.path)

import db_info#@UnresolvedImport

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from flask import Flask,make_response,request as flreq
from flask_restful import Resource, Api

opttab = 'sec_schema.options_table'
futtab = 'sec_schema.underlying_table'

class SqlDownloader():
    def __init__(self,file_name,config_name):
        self.file_name = file_name
        self.pga = db_info.get_db_info(config_name=config_name)


    def get_options(self,symbol,yyyymmdd_beg=None,yyyymmdd_end=None):
        sql = f"select * from {opttab} where symbol='{symbol}'"
        if yyyymmdd_beg is not None:
            sql += f' and settle_date>={yyyymmdd_beg}'
        if yyyymmdd_end is not None:
            sql += f' and settle_date<={yyyymmdd_end}'
        df =  self.pga.get_sql(sql)
        if df is not None and len(df)>2:
            df = df.sort_values(['pc','settle_date','strike'])
        return df

    def create_csvdownloader(self): 
        parent_self = self
        class HelloWorld(Resource):
            def get(self):
                print('entering HelloWorld')
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
        return HelloWorld

if __name__=='__main__':
    # argv[1] = port, argv[2] = config_name of db
    # get pga
    app_host = '127.0.0.1'
    argvs = sys.argv
    app_port = int(argvs[1])
    config_name = argvs[2]
    server = Flask('sec_db')
    url_base_pathname=f'/app{app_port}/'
    app = dash.Dash(server=server,prevent_initial_callbacks=True,url_base_pathname=url_base_pathname)
    api = Api(server)
    sqld = SqlDownloader('mycsv',config_name)
    api.add_resource(sqld.create_csvdownloader(), f'/app{app_port}/csv')
    app.layout = html.Div([])
    app.run_server(host=app_host,port=app_port)
