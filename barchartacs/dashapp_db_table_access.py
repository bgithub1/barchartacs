'''
Created on Aug 2, 2020

Use dash_extensions to query and display large DataFrames that come from Postgres DB queries

Component flow:
    1. select_input: dcc.Input, into which you type an sql select statement (but WITHOUT the word select)
        e.g.: * from sec_schema.options_table limit 100000
    2. 
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
import zipfile
import io
import base64
import datetime
import pytz
import re
import db_info#@UnresolvedImport

args = sys.argv
config_name=None
if len(args)>1:
    config_name = args[1]

app_port = 8812
if len(args)>2:
    app_port = int(args[2])

# get pga
pga = db_info.get_db_info(config_name=config_name)
ROWS_FOR_DASHTABLE=500
MAIN_ID = 'tdb'

# Create app.
# url_base_pathname=f'/app{app_port}/'
# app = Dash(prevent_initial_callbacks=True,url_base_pathname=url_base_pathname)
# app.title='db_table_access'
# server = app.server

DEFAULT_TIMEZONE = 'US/Eastern'

def make_text_centered_div(text):    
    col_inner_style = {
        'margin':'auto',
        'word-break':'break-all',
        'word-wrap': 'break-word'
    }
    return html.Div([text],style=col_inner_style)

def parse_contents(contents):
    '''
    app.layout contains a dash_core_component object (dcc.Store(id='df_memory')), 
      that holds the last DataFrame that has been displayed. 
      This method turns the contents of that dash_core_component.Store object into
      a DataFrame.
      
    :param contents: the contents of dash_core_component.Store with id = 'df_memory'
    :returns pandas DataFrame of those contents
    '''
    c = contents.split(",")[1]
    c_decoded = base64.b64decode(c)
    c_sio = io.StringIO(c_decoded.decode('utf-8'))
    df = pd.read_csv(c_sio)
    # create a date column if there is not one, and there is a timestamp column instead
    cols = df.columns.values
    cols_lower = [c.lower() for c in cols] 
    if 'date' not in cols_lower and 'timestamp' in cols_lower:
        date_col_index = cols_lower.index('timestamp')
        # make date column
        def _extract_dt(t):
            y = int(t[0:4])
            mon = int(t[5:7])
            day = int(t[8:10])
            hour = int(t[11:13])
            minute = int(t[14:16])
            return datetime.datetime(y,mon,day,hour,minute,tzinfo=pytz.timezone(DEFAULT_TIMEZONE))
        # create date
        df['date'] = df.iloc[:,date_col_index].apply(_extract_dt)
    return df
def transformer_csv_from_upload_component(contents):
    '''
    Convert the contents of the file that results from use of dash_core_components.Upload class.  
        This method gets called from the UploadComponent class callback.
    :param contents:    The value received from an update of a dash_core_components.Upload instance.'
                            
    '''
    if contents is None or len(contents)<=0 or contents[0] is None:
        d =  None
    else:
        d = parse_contents(contents).to_dict('rows')
    return d

# method to create a data frame from zipfile contents that came from an dcc.Upload component
def zipdata_to_df(contents,filename):
    c = contents.split(",")[1]
    content_decoded = base64.b64decode(c)
#         content_decoded = base64.b64decode(contents)
    # Use BytesIO to handle the decoded content
    zoio2 = io.BytesIO(content_decoded)
    f = zipfile.ZipFile(zoio2).open(filename.replace('.zip',''))
    nym2 = [l.decode("utf-8")  for l in f]
    sio2 = io.StringIO()
    sio2.writelines(nym2)
    sio2.seek(0)
    df = pd.read_csv(sio2)
    return df

class ZipAccess(html.Div):
    def _mkid(self,s):
        return f"{self.main_id}_{s}"        
    
    def __init__(self,app,main_id):
        self.main_id = main_id
        self.app = app
        self.btn_run = html.Button(
            children="Upload Zip or CSV",id=self._mkid('btn_run'),
            )
        self.num_rows_input = dcc.Input(
            id=self._mkid('num_rows_input'),debounce=True,
            value=ROWS_FOR_DASHTABLE,type='number',
            min=100, max=ROWS_FOR_DASHTABLE*2, step=100,
            )
        # create uploader
        uploader_text = make_text_centered_div("Choose a Zipped CSV File")
        self.uploader_comp = dcc.Upload(
                    id=self._mkid("uploader_comp"),
                    children=uploader_text,
                    accept = '.zip' ,
#                     style = {"background-color": "#e7e7e7", 
#                              "color": "black",
#                              "overflow-wrap": "normal",
#                              }
                    )
        self.uploader_file_path = html.Div(id=self._mkid('uploader_file_path'))
        self.main_store = dcc.Store(id=self._mkid('main_store'))
        children =[
                html.Div([
                        self.btn_run,
                        self.uploader_comp,
                        self.num_rows_input,
                        html.Div()
                        ],
                        style={"display":"grid","grid-template-columns":'1fr 1fr 1fr 7fr',
                               'grid-template-rows':'1fr'}
                    ),
                dcc.Loading(
                    children=[self.main_store], 
                    fullscreen=True, type="dot"
                )
            ]      
        
        super(ZipAccess,self).__init__(children,id=self._mkid('zip_div'))

    def register_app(self,theapp):
        @theapp.callback(
            [Output(self.uploader_file_path.id,'children')],
            [Input(self.uploader_comp.id,'filename')]
            )
        def _update_filename(filename):
            if filename is None or len(filename)<1:
                raise PreventUpdate('_update_filename: no filename')
            return filename.split(",")[-1]
        
        @theapp.callback(
                [ServersideOutput(self.main_store.id,'data')],
                [Trigger(self.btn_run.id,'n_clicks')],
                [State(self.uploader_comp.id,'contents'),
                 State(self.uploader_comp.id,'filename')
                 ]
#                 [ServersideOutput(self.main_store.id,'data')],
#                 [Trigger(self.uploader_comp.id,'filename')],
#                 [State(self.uploader_comp.id,'contents'),
#                 State(self.uploader_comp.id,'filename')]
            )
        def _update_main_store(contents,filename):
            print(f"_update_main_store: {datetime.datetime.now()} filename: {filename}")
            if contents is None:
                raise PreventUpdate('no data uploaded yet')
            if filename is None:
                raise PreventUpdate('no filename yet')
            if len(re.findall("\.zip$",filename.lower()))>0:
                # it's a zip file
                df = zipdata_to_df(contents, filename)
            else:
                table_data_dict = transformer_csv_from_upload_component(contents)
                df = pd.DataFrame(table_data_dict).head(1)
            print(f"_update_main_store")
            return df
            
        return _update_filename,_update_main_store   
        
        
class SqlAccess(html.Div):
    def _mkid(self,s):
        return f"{self.main_id}_{s}"        
    
    def __init__(self,app,main_id):
        self.main_id = main_id
        self.app = app
        self.btn_run = html.Button(
            children="RUN SQL",id=self._mkid('btn_run'),
            )
        self.num_rows_input = dcc.Input(
            id=self._mkid('num_rows_input'),debounce=True,
            value=ROWS_FOR_DASHTABLE,type='number',
            min=100, max=ROWS_FOR_DASHTABLE*2, step=100,
            )
        # create input box for sql select (WITHOUT THE WORD SELECT)
        self.select_input = dcc.Input(
            id=self._mkid('select_input'),
            placeholder="Enter sql select statement (without the word select)",
            )
        self.main_store = dcc.Store(id=self._mkid('main_store'))
        children =[
                html.Div([
                        self.btn_run,
                        self.num_rows_input,
                        self.select_input
                        ],
                        style={"display":"grid","grid-template-columns":'1fr 1fr 8fr',
                               'grid-template-rows':'1fr'}
                    ),
                dcc.Loading(
                    children=[self.main_store], 
                    fullscreen=True, type="dot"
                )
            ]      
        super(SqlAccess,self).__init__(children,id=self._mkid('sql_div'))
        
        
    def register_app(self,theapp):        
        @theapp.callback([ServersideOutput(self.main_store.id, "data")],
                      Trigger(self.btn_run.id, "n_clicks"),
                      State(self.select_input.id,'value'))
        def _query(sql):
            print(f"_query sql: {sql}")
            if sql is None or len(sql)<1:
                raise PreventUpdate('no sql query')
            df = pga.get_sql(f"select {sql}")
            cols = list(df.columns.values)
            df['rownum'] = list(range(1,len(df)+1))
            df = df[['rownum'] + cols]
            return df
        return _query
    
    

def filter_df(df,col,operator,predicate):
    if (predicate is None) or (len(predicate.strip())<=0):
        return None
    if (col is None) or (len(col.strip())<=0):
        return None
    p = predicate
    # Check to see if the col holds string data, and if so
    #   wrap the predicate in single quotes
    o = operator
    if (o is None) or (len(o.strip())<=0):
        o = 'contains'
    if o == 'contains':
        df = df[df[col].astype(str).str.contains(p)]
    else:
        needs_quotes = False
        try:
            df[col].astype(float).sum()
        except:
            needs_quotes = True
        p = f"'{p}'" if needs_quotes else p
        q = f"{col} {o} {p}"
        df = df.query(q)
    return df


_query_operators_options = [
    {'label':'','value':''},
    {'label':'=','value':'=='},
    {'label':'!=','value':'!='},
    {'label':'>','value':'>'},
    {'label':'<','value':'<'},
    {'label':'>=','value':'>='},
    {'label':'<=','value':'<='},
    {'label':'btwn','value':'btwn'},
]
class DtChooser(dash_table.DataTable):
    '''
    Create a Dash DataTable that displays filter options for the columns 
      of another DataFrame's data, that is located in a dcc.Store
    '''
    def __init__(self,dt_chooser_id,
                 main_store,num_filters_rows=4,logger=None):
        '''
        
        :param dt_chooser_id: DataTable id
        :param store_of_df_data: dcc.Store object that holds the dict of the DataFrame that will be filtered
        :param logger: instance of logging, usually obtained from init_root_logger
        '''
        self.main_store = main_store
        self.num_filters_rows = num_filters_rows
        self.logger = logger
        
        super(DtChooser,self).__init__(
            id=dt_chooser_id,
            #data=df.to_dict('records'),
            columns=[
                {'id': 'column', 'name': 'column', 'presentation': 'dropdown'},
                {'id': 'operation', 'name': 'operation', 'presentation': 'dropdown'},
                {'id': 'operator', 'name': 'operator'},
            ],
            editable=True,
        )
        
#         self.dashlink = DashLink([(store_of_df_data.id,'data')],[(self.id,'dropdown'),(self.id,'data')],_update_column_dropdown_options)
    def register_app(self,theapp):
        @theapp.callback(
            [Output(self.id,'dropdown'),Output(self.id,'data')],
            [Input(self.main_store.id,'data')]
        )
        def _update_filters(df):
            if df is None:
                raise PreventUpdate("DtChooser._update_column_dropdown_options: no input data for columns")
            newcols = df.columns.values
            thisdd = {
                'operation': {'options': _query_operators_options},
                'column':{'options': [{'label': i, 'value': i} for i in newcols]}
            }
            blanks = ['' for _ in range(self.num_filters_rows)]
            df_filter = pd.DataFrame({'column':blanks,'operation':blanks,'operator':blanks})
            return [thisdd,df_filter.to_dict('records')]
        return _update_filters
        

    def execute_filter_query(self,df_source,df_query):
        '''
        Filter df_source - IN PLACE - using df_query.
        df_query has 3 columns: column, operation, operator
        :param df_source: orignal DataFrame to be queried
        :param df_query: a DataFrame of queries
        '''
        for i in range(len(df_query)):
            s = df_query.iloc[i]
            predicate = s.operator
            if (predicate is None) or (len(predicate.strip())<=0):
                continue
            c = s['column']
            o = s.operation
            df_temp = filter_df(df_source,c,o,predicate)
            if df_temp is not None:
                df_source = df_temp.copy()
        return df_source



class CsvViewer(html.Div):
    def __init__(self,app,main_id,
                 main_store,num_rows_input=None,
                 logger=None,button_style=None):
        self.main_id = main_id
        self.app = app
        if num_rows_input is None:
            num_rows_store = dcc.Store(id=self._mkid("num_rows_store"),data=500)
            self.num_rows_input = Input(num_rows_store.id,"data")
        else:
            self.num_rows_input = num_rows_input
            
        self.main_store = main_store
        
        self.filter_btn = html.Button(
            id=self._mkid('filter_button'),
            children= "RUN FILTER",
            style=button_style
#             style = dict(width = '10%',display = 'table-cell')
            )
        
        self.dtc = DtChooser(
            self._mkid('dtc'),
            self.main_store,
            num_filters_rows=4,
            logger=logger)
        
        self.dt_data = self._make_dt(
            self._mkid('dt_data'),pd.DataFrame(),
            displayed_rows=ROWS_FOR_DASHTABLE,page_action='custom'
        )

        self.dt_data_div = html.Div([self.dt_data],self._mkid('dt_data_div'))
        
        filter_grid = html.Div(
            [self.filter_btn,self.dtc],
            style={'display':'grid','grid-template-columns':'10% 90%',
                   'grid-template-rows':'1fr'}
            )
        
        children =  html.Div([
                            filter_grid,
                            dcc.Loading(
                                children=[self.dt_data_div], 
                                fullscreen=True, type="dot"
                            )
                        ],
#                         style={'display':'grid',
#                                'grid-template-rows':'1fr 30fr',
#                                'grid-template-columns':'1fr'}
            
                    )
           
        super(CsvViewer,self).__init__(children,id=self._mkid('csv_viewer_div'))
        
    def _mkid(self,s):
        return f"{self.main_id}_{s}"        

    def _make_dt(self,dt_id,df,displayed_rows=100,page_action='native'):
        dt = dash_table.DataTable(
            id=dt_id,
            page_current= 0,
            page_size=displayed_rows,
            page_action=page_action, 
            style_table={
                'overflowY':'scroll',
                'overflowX':'scroll',
                'height': 'auto'
            } ,
#             virtualization=True,
#             fixed_rows={'headers': True},
        )
        dt.data=df.to_dict('rows')
        dt.columns=[{"name": i, "id": i} for i in df.columns.values]                    
        return dt

    def register_app(self,theapp):
        @theapp.callback(
            [Output(self.dt_data.id,'page_size')],
            [self.num_rows_input]
        )
        def _update_page_size(value):
            print(f"_update_page_size: {value}")
            return value
        
        
        @theapp.callback([Output(self.dt_data.id,'data'),
                          Output(self.dt_data.id,'columns'),
                       Output(self.dt_data.id,'page_count')], 
                      [Input(self.main_store.id, "data"),
                       Input(self.dt_data.id,'page_current'),
                       Input(self.filter_btn.id,'n_clicks')],
                      [State(self.dt_data.id,'page_size'),
                       State(self.dtc.id,'data')])
        def display_df(df, page_current,n_clicks,page_size,dtc_query_dict_df):
            print(f"entering display_df: {datetime.datetime.now()} {len(df)} {page_size}")
            pagcur = int(str(page_current))
            if (pagcur is None) or (pagcur<0):
                pagcur = 0
            ps = ROWS_FOR_DASHTABLE
            
            if (page_size is not None):
                ps = int(str(page_size))
            df_after_filter = df
            if (dtc_query_dict_df is not None):
                df_dtc = pd.DataFrame(dtc_query_dict_df)
                if len(df_dtc)>0:
                    df_after_filter = self.dtc.execute_filter_query(
                        df,df_dtc
                        )                    
            
            beg_row = pagcur*ps
            if pagcur*ps > len(df):
                beg_row = len(df) - ps
        
#             dict_data = df.iloc[beg_row:beg_row + ps].to_dict('records')
            dict_data = df_after_filter.iloc[beg_row:beg_row + ps].to_dict('records')
            cols = [{"name": i, "id": i} for i in df.columns.values]
#             page_count = int(len(df)/ps) + (1 if len(df) % ps > 0 else 0)
            page_count = int(len(df_after_filter)/ps) + (1 if len(df_after_filter) % ps > 0 else 0)
            return dict_data,cols,page_count
        
        dtc_callback = self.dtc.register_app(theapp)
        return _update_page_size,display_df,dtc_callback


