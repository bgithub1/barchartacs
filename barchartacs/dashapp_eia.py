'''
Created on Aug 19, 2020

@author: bperlman1
'''

import sys
import dash_table
import numpy as np
import pandas as pd

import dash_core_components as dcc
import dash_html_components as html
from dash_extensions.enrich import Dash, ServersideOutput, Output, Input, State, Trigger
from dash.exceptions import PreventUpdate
import progressive_dropdown as progdd#@UnResolvedImport
args = sys.argv

def _make_dt(dt_id,df_pet=None,displayed_rows=100,page_action='native'):
    dt = dash_table.DataTable(
        id=dt_id,
        page_current= 0,
        page_size=displayed_rows,
        page_action=page_action, 
        sort_action='native',
        sort_mode='multi',
        sort_by=[],
        style_table={
            'overflowY':'scroll',
            'overflowX':'scroll',
            'height': 'auto'
        } ,
    )
    df = pd.DataFrame() if df_pet is None else df_pet
    dt.data=df.to_dict('records')
    dt.columns=[{"name": i, "id": i} for i in df.columns.values]                    
    return dt


class HtmlDiv(html.Div):
    def _mkid(self,s):
        return f"{self.main_id}_{s}"        
    def __init__(self,main_id,children,**args):
        self.main_id = main_id
        super(HtmlDiv,self).__init__(children,id=self._mkid('main_div'),**args)
        
class EiaAccess(HtmlDiv):
    def __init__(self,main_id,input_pet_file='./temp_folder/df_pet_lower.csv'):
        super(EiaAccess,self).__init__(main_id,None)
        self.main_store = dcc.Store(id=self._mkid('main_store'))
        self.trigger_interval = dcc.Interval(
            id=self._mkid('trigger_interval'),
            interval =1000,max_intervals=1
            )
        self.trigger_button = html.Button(
            id=self._mkid('trigger_button'),children='Start App')
        self.num_displayable_columns = dcc.Input(
            id=self._mkid('num_displayable_columns'), value=20,type='number',
            debounce=True
            )
        self.data_dt = _make_dt(self._mkid('data_dt'),)
        self.input_pet_file = input_pet_file
        self.children = dcc.Loading(
            children=[self.trigger_interval,self.num_displayable_columns,
                      self.data_dt,self.main_store],
            fullscreen=True, type="dot"
            )

    def register_app(self,theapp):
        @theapp.callback(
                [ServersideOutput(self.main_store.id,'data')],
                [Trigger(self.trigger_interval.id,'n_intervals')]
            )
        def _update_main_store():
            print(f"EiaAccess._update_main_store")
            df = pd.read_csv(self.input_pet_file)
            return df

        @theapp.callback(
            [
                Output(self.data_dt.id,'data'),
                Output(self.data_dt.id,'columns')
            ],
            [
                Input(self.main_store.id,'data'),
                Input(self.num_displayable_columns.id,'value')
                ]
            )
        def _populate_data_dt(df_main_store_data,num_displayable_columns):
            columns = df_main_store_data.columns.values[:num_displayable_columns]
            print(f"EiaAccess._populate_data_dt number of columns:{len(df_main_store_data.columns.values)}")
            
            df_ret = df_main_store_data[columns]
            ret_columns = [{'label':c,'id':c} for c in columns]
            dict_ret = df_ret.to_dict('records')
            return dict_ret,ret_columns
        
        print('registered eia_access')
        
app_port = 8813

url_base_pathname=f'/app{app_port}/'
app = Dash(prevent_initial_callbacks=True,url_base_pathname=url_base_pathname)
app.title='Eia Viewer'

eia_access = EiaAccess('eia_access')
app_layout = dcc.Loading(children=[eia_access],
                         fullscreen=True, type="dot")

app.layout = app_layout
eia_access.register_app(app)

if __name__ == '__main__':
    app.run_server(port=app_port,debug=False)


    