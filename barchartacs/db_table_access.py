'''
Created on Aug 2, 2020

@author: bperlman1
'''
import sys, os
import dash_core_components as dcc
import dash_html_components as html
from dash_extensions.enrich import Dash, ServersideOutput, Output, Input, State, Trigger
from dash.exceptions import PreventUpdate
import dash_table
import numpy as np
import pandas as pd
thisdir = os.path.abspath('.')
thisparentdir = os.path.abspath('../')
if thisdir not in sys.path:
    sys.path.append(thisdir)
if thisparentdir not in sys.path:
    sys.path.append(thisparentdir)        
from barchartacs import db_info

args = sys.argv
config_name=None
if len(args)>1:
    config_name = args[1]

app_port = 8812
if len(args)>2:
    app_port = int(args[2])

# get pga
pga = db_info.get_db_info(config_name=config_name)
ROWS_FOR_DASHTABLE=1000
MAIN_ID = 'tdb'

# Create app.
url_base_pathname=f'/app{app_port}/'
app = Dash(prevent_initial_callbacks=True,url_base_pathname=url_base_pathname)
app.title='db_table_access'
server = app.server


def _mkid(s,main_id=MAIN_ID):
    return f"{main_id}_{s}"

def _make_df(value):
    dict_df = {'rownum':list(range(1,value+1))}
    for i in range(1,11):
        d = np.random.rand(int(value))
        dict_df[f'c{i}']=d
    df = pd.DataFrame(dict_df)
    return df


def _make_dt(dt_id,df,displayed_rows=100,page_action='native'):
    dt = dash_table.DataTable(
        id=dt_id,
        page_current= 0,
        page_size=displayed_rows,
        page_action=page_action, 
    )
    dt.data=df.to_dict('rows')
    dt.columns=[{"name": i, "id": i} for i in df.columns.values]                    
    return dt

# create input box for sql select (WITHOUT THE WORD SELECT)
select_input = dcc.Input(
    id=_mkid('select_input'),debounce=True,
    placeholder="Enter sql select statement (without the word select)",
    style = dict(width = '50%',display = 'table-cell')
)

main_store = dcc.Store(id=_mkid('main_store'))
dt_data = _make_dt(
    _mkid('dt_data'),pd.DataFrame(),
    displayed_rows=ROWS_FOR_DASHTABLE,page_action='custom'
)

dt_data_div = html.Div([dt_data],_mkid('dt_data_div'))

app.layout = html.Div([select_input, 
    dcc.Loading(children=[main_store,dt_data_div], fullscreen=True, type="dot")])

@app.callback([ServersideOutput(main_store.id, "data")],
              Trigger(select_input.id, "n_submit"),
              State(select_input.id,'value'))
def _query(sql):
    print(f"_query sql: {sql}")
    if sql is None or len(sql)<1:
        raise PreventUpdate('no sql query')
    df = pga.get_sql(f"select {sql}")
    cols = list(df.columns.values)
    df['rownum'] = list(range(1,len(df)+1))
    df = df[['rownum'] + cols]
    return df

@app.callback([Output(dt_data.id,'data'),Output(dt_data.id,'columns'),
               Output(dt_data.id,'page_count')], 
              [Input(main_store.id, "data"),Input(dt_data.id,'page_current'),
               Input(dt_data.id,'page_size')])
def display_df(df, page_current,page_size):
    pagcur = page_current
    if (pagcur is None) or (pagcur<0):
        pagcur = 0
    ps = page_size
    if (ps is None) or (ps<1):
        ps = ROWS_FOR_DASHTABLE
    beg_row = page_current*page_size
    if pagcur*ps > len(df):
        beg_row = len(df) - ps

    dict_data = df.iloc[beg_row:beg_row + ps].to_dict('records')
    cols = [{"name": i, "id": i} for i in df.columns.values]
    page_count = int(len(df)/ps) + (1 if len(df) % ps > 0 else 0)
    return dict_data,cols,page_count

if __name__ == '__main__':
    app.run_server(port=app_port)
        