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
import flask
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
from dashapp import dashapp2 as dashapp#@UnResolvedImport
import progressive_dropdown as progdd#@UnResolvedImport
import logging
import base64
import io
import zipfile
import spacy
import pytextrank



args = sys.argv

configs = open('./temp_folder/dashapp_db_table_config_name.txt','r').read().split(',')
config_name=configs[0]

# app_port = 8812
# if len(configs)>1:
#     app_port=int(configs[1])
    
# get pga
pga = db_info.get_db_info(config_name=config_name)
ROWS_FOR_DASHTABLE=500
MAIN_ID = 'tdb'

DEFAULT_LOG_PATH = './logfile.log'
DEFAULT_LOG_LEVEL = 'INFO'

def create_nlp_doc(df):
    
    reports_text = '\n'.join(df.apply(lambda r:' '.join(r.values.astype(str)),axis=1).values)[:999999]

    # example text
    text = reports_text
    # load a spaCy model, depending on language, scale, etc.
    nlp = spacy.load("en_core_web_sm")#@UnDefinedVariable

    # add PyTextRank to the spaCy pipeline
    tr = pytextrank.TextRank()
    nlp.add_pipe(tr.PipelineComponent, name="textrank", last=True)

    doc = nlp(text)

    # examine the top-ranked phrases in the document
    df_docs = pd.DataFrame(
        {
            'prank':[p.rank for p in doc._.phrases],
            'pcount':[p.count for p in doc._.phrases],
            'ptext':[p.text for p in doc._.phrases],
            'pcomb':[p.rank*p.count for p in doc._.phrases]
        }
    )
    return df_docs


def df_to_zipiofile(df,filename):
    sio2 = io.StringIO()
    df.to_csv(sio2,index=False)
    sio2.seek(0)
    zoio2 = io.BytesIO()
    f = zipfile.ZipFile(zoio2,'a',zipfile.ZIP_DEFLATED,False)
    f.writestr(filename,sio2.read())
    f.close() 
    zoio2.seek(0)
    return zoio2

def df_to_zipfile(df,filename,fullpath):
    zz = df_to_zipiofile(df,filename)
    ff = open(fullpath,'wb')
    ff.write(zz.getbuffer())
    ff.close()  
      
def init_root_logger(logfile=DEFAULT_LOG_PATH,logging_level=None):
    level = logging_level
    if level is None:
        level = logging.INFO
    # get root level logger
    logger = logging.getLogger()
    if len(logger.handlers)>0:
        return logger
    logger.setLevel(logging.getLevelName(level))

    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)   
    return logger


DEFAULT_TIMEZONE = 'US/Eastern'


# def create_unique_values(df):
#     # create a list of tuples, where each tuple has a column name and a list of unique values
#     vals = [(c,list(df[c].unique())) for c in df.dtypes[(df.dtypes=='object') | (df.dtypes=='str')].index]
#     # if there are no tuples in the list, return an empty DataFrame
#     if len(vals)<1:
#         return pd.DataFrame()
#     # create a one row DataFrame with the first column, and the unique values for that column
#     df_unique = pd.DataFrame({vals[0][0]:vals[0][1]})
# 
#     # Loop through the remaining columns, using pd. concat to add new columns
#     #   to df_unique, which will automatically broadcast each column so that 
#     #   all columns have the same length
#     for v in vals[1:]:
#         df_unique = pd.concat([df_unique,pd.DataFrame({v[0]:v[1]})],axis=1)
#     return df_unique


def create_unique_values(df):
    # create a list of tuples, where each tuple has a column name and a list of unique values
    vals = [(c,list(df[c].unique())) for c in df.dtypes[(df.dtypes=='object') | (df.dtypes=='str')].index]
    # if there are no tuples in the list, return an empty DataFrame
    if len(vals)<1:
        return pd.DataFrame()
    # create a one row DataFrame with the first column, and the unique values for that column
    df_unique = pd.DataFrame({vals[0][0]:vals[0][1]})

    # Loop through the remaining columns, using pd. concat to add new columns
    #   to df_unique, which will automatically broadcast each column so that 
    #   all columns have the same length
    for v in vals[1:]:
        df_unique = pd.concat([df_unique,pd.DataFrame({v[0]:v[1]})],axis=1)
    df_unique = df_unique.fillna('')
    df_unique = df_unique[df_unique.apply(lambda r:len(''.join(r))>0,axis=1)]
    df_unique.index = list(range(len(df_unique)))
    return df_unique


def create_aggregate_summary(df,rounding=4):
    try:
        cols = df.columns.values
        num_cols = [c for c in cols if (df[c].dtype=='float64') or (df[c].dtype=='int64')]
    except Exception as e:
        print(f'create_aggregate_summary Exception: {str(e)}')
        print(f'create_aggregate_summary df')
        print(df)
        return pd.DataFrame()
    df_ret = df[num_cols].describe().transpose()
    ret_cols = ['col'] + list(df_ret.columns.values)
    df_ret['col'] = df_ret.index.values
    df_ret = df_ret.reset_index()
    df_ret = df_ret[ret_cols]
    return df_ret.round(rounding)

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
    # Use BytesIO to handle the decoded content
    zoio2 = io.BytesIO(content_decoded)
    f = zipfile.ZipFile(zoio2).open(filename.replace('.zip',''))
    nym2 = [l.decode("utf-8")  for l in f]
    sio2 = io.StringIO()
    sio2.writelines(nym2)
    sio2.seek(0)
    df = pd.read_csv(sio2)
    return df

def _make_dt(dt_id,df,displayed_rows=100,page_action='native'):
    dt = dash_table.DataTable(
        id=dt_id,
        page_current= 0,
        page_size=displayed_rows,
        page_action=page_action, 
        sort_action='custom',
        sort_mode='multi',
        sort_by=[],
        style_table={
            'overflowY':'scroll',
            'overflowX':'scroll',
            'height': 'auto'
        },
        style_cell={
        'whiteSpace': 'normal',
        'height': 'auto',
        },
        
    )
    dt.data=df.to_dict('rows')
    dt.columns=[{"name": i, "id": i} for i in df.columns.values]                    
    return dt



class XyGraphDefinition(html.Div):
    def __init__(self,data_store,div_id,num_graph_filter_rows=2,
                 logger=None):
        titles = ['X Column','Y Left Axis','Y Right Axis']
        self.prog_dd = progdd.ProgressiveDropdown(
            data_store,f'{div_id}_dropdowns',
            len(titles),title_list=titles,use_title_divs=False
            )
        # create input divs for inputing y left and right axis titles
        y_left_axis_input = dcc.Input(value="Y MAIN",id=f"{div_id}_y_left_axis_input")
        y_left_axis_input_title = dashapp.make_text_centered_div("Y Left Axis")
        y_left_axis_div = dashapp.multi_row_panel([y_left_axis_input_title,y_left_axis_input])

        y_right_axis_input = dcc.Input(value="Y ALT",id=f"{div_id}_y_right_axis_input")
        y_right_axis_input_title = dashapp.make_text_centered_div("Y Right Axis")
        y_right_axis_div = dashapp.multi_row_panel([y_right_axis_input_title,y_right_axis_input])

        # create the graph button
        self.graph_button = html.Button('Click for Graph',id=f'{div_id}_graph_button')
        graph_button_title = dashapp.make_text_centered_div('Refresh Graph')
        self.graph_button_div = dashapp.multi_row_panel([graph_button_title,self.graph_button])
        
        # create the graph title
        graph_title_input = dcc.Input(value="XY Graph",id=f"{div_id}_graph_title")
        graph_title_title = dashapp.make_text_centered_div("Graph Title")
        graph_title_div = dashapp.multi_row_panel([graph_title_title,graph_title_input])
        
        
        # create the Graph Component, with no graph in it as of yet
        self.graph_comp = dcc.Graph(id=f"{div_id}_graph")
        
        # build graph DashLink
        dd_states = [State(dd.id,'value') for dd in self.prog_dd.pd_dd_list]
        store_state = [State(data_store.id,'data')]
        axis_states = [
            State(y_left_axis_input.id,'value'),
            State(y_right_axis_input.id,'value'),
            State(graph_title_input.id,'value')
            ]
        self.states =  dd_states  + store_state + axis_states
        
        filter_rows = self.prog_dd.pd_div_list + [y_left_axis_div,y_right_axis_div] + [self.graph_button_div,graph_title_div]
        fr1 = dashapp.multi_column_panel(filter_rows[0:3])
        fr2 = dashapp.multi_column_panel(filter_rows[3:6])
        fr3 = dashapp.multi_column_panel(filter_rows[6:7])
#         filter_div =dashapp.multi_row_panel([fr1,fr2,fr3])
        filter_div =html.Div([fr1,fr2,fr3])
        super(XyGraphDefinition,self).__init__(
            [filter_div,self.graph_comp],id=div_id,
            style={
                'display':'grid',
                'grid-template-rows':'1fr 3fr',
                'grid-templage-columns':'1fr'
                }
            )
        
        
    def register_app(self,theapp):
        @theapp.callback(
            Output(self.graph_comp.id,'figure'),
            Input(self.graph_button.id,'n_clicks'),
            self.states
            )
        def _build_fig_callback(
                _,
                x_col,
                y_left_cols,
                y_right_cols,
                dict_df,
                y_left_label,
                y_right_label,
                graph_title
                ):
            if type(x_col) == list:
                x_col = x_col[0]
            if any([x_col is None,y_left_cols is None]):
                raise PreventUpdate("No columns selected for graph")
                
            df = pd.DataFrame(dict_df)
            
            yrc = [] if (y_right_cols is None) or (len(y_right_cols)<1) else y_right_cols
            df = df[[x_col] + y_left_cols + yrc]
            fig = dashapp.plotly_plot(
                df_in=df,x_column=x_col,yaxis2_cols=y_right_cols,
                y_left_label=y_left_label,y_right_label=y_right_label,
                plot_title=graph_title)
            fig.update_layout(autosize=True,hovermode='x unified',
                              hoverlabel = dict(namelength = -1))
            return fig
        self.prog_dd.register_app(theapp)

class ColumnSelector(dash_table.DataTable):
    def __init__(self,dt_id,options=None,
                 options_input_dashtable=None,
                 displayed_rows=4,
                 value=None,style=None):
        
        self.options_input_dashtable=options_input_dashtable
        opts = options
        if opts is None:
            opts = []
        df = pd.DataFrame({'option':opts})            
        data=df.to_dict('rows')
        columns=[{"name": i, "id": i} for i in df.columns.values]                    
        selected_rows=list(range(len(df)))
        
        super(ColumnSelector,self).__init__(
            id=dt_id,
            editable=True,
            page_action='none', 
            style_table={
                'overflowY':'auto',
                'height': f'{30*(displayed_rows+1)+2}px'
            } ,
            fixed_rows={'headers': True},
            row_selectable='multi',
            data=data,
            columns=columns,
            selected_rows=selected_rows
        )
    def register_app(self,theapp):
        if self.options_input_dashtable is not None:
            @theapp.callback(
                [
                    Output(self.id,'data'),
                    Output(self.id,'columns'),
                    Output(self.id,'selected_rows')],
                [Input(self.options_input_dashtable.id,'columns')]            
            )
            def _change_options(columns_dict):
                if columns_dict is None or len(columns_dict)<=0:
                    raise PreventUpdate("callback MultiDropdown._change_options: no DataFrame columns")
                names = [c['name'] for c in columns_dict]
                df_return = pd.DataFrame({'option':names})
                data = df_return.to_dict('records')
                columns = [{'name':c,'id':c} for c in df_return.columns.values]
                selected_rows=df_return.index.values
                return data,columns,selected_rows
            return _change_options
        else:
            return None
                
            

class ZipAccess(html.Div):
    def _mkid(self,s):
        return f"{self.main_id}_{s}"        
    
    def __init__(self,app,main_id,zip_or_csv='zip',logger=None):
        '''
        
        :param app:
        :param main_id:
        :param zip_or_csv:
        :param logger:
        '''
        self.main_id = main_id
        self.save_store = dcc.Store(id=self._mkid("save_store"))
        self.app = app
        self.logger = init_root_logger() if logger is None else logger
        self.zip_or_csv = zip_or_csv
        self.btn_run = html.Button(
            children=f"Upload {zip_or_csv.upper()}",id=self._mkid('btn_run'),
            style={"border":"1px black solid"}
            )
        self.num_rows_input = dcc.Input(
            id=self._mkid('num_rows_input'),debounce=True,
            value=ROWS_FOR_DASHTABLE,type='number',
            min=100, max=ROWS_FOR_DASHTABLE*2, step=100,
            style={"border":"1px black solid"}
            )
        # create uploader
        uploader_text = make_text_centered_div(f"Choose a {zip_or_csv.upper()} File")
        self.uploader_comp = dcc.Upload(
                    id=self._mkid("uploader_comp"),
                    children=uploader_text,
                    accept = f'.{zip_or_csv.lower()},.{zip_or_csv.upper()}' ,
            style={"border":"1px black solid",'textAlign': 'center',}
                    )
        self.uploader_file_path = html.Div(
            id=self._mkid('uploader_file_path'),
            style={"border":"1px black solid"}
            )
        self.main_store = dcc.Store(id=self._mkid('main_store'))
        
        children =[
                html.Div([
                        self.btn_run,
                        html.Div(),
                        self.uploader_comp,
                        html.Div(),
                        self.uploader_file_path,
                        html.Div(),
                        self.num_rows_input,
                        ],
                        style={"display":"grid","grid-template-columns":'3fr 1fr 3fr 1fr 3fr 1fr 3fr',
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
            )
        def _update_main_store(contents,filename):
            self.logger.debug(f"_update_main_store: {datetime.datetime.now()} filename: {filename}")
            if contents is None:
                raise PreventUpdate('no data uploaded yet')
            if filename is None:
                raise PreventUpdate('no filename yet')
            if len(re.findall("\.zip$",filename.lower()))>0:
                # it's a zip file
                df = zipdata_to_df(contents, filename)
            else:
                table_data_dict = transformer_csv_from_upload_component(contents)
                df = pd.DataFrame(table_data_dict)
            self.logger.debug(f"_update_main_store")
            return df
            
        @theapp.callback(
            Output(self.save_store.id,'data'),
            Input(self.uploader_comp.id,'filename')
            )
        def _update_save(filename):
            return {'filename':filename}
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
        df = df[df[col].astype(str).str.contains(p)]
    elif o == 'not':
        df = df[~df[col].astype(str).str.contains(p)]
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

def execute_filter_query(df_source,df_query):
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

_query_operators_options = [
    {'label':'','value':''},
    {'label':'=','value':'=='},
    {'label':'!=','value':'!='},
    {'label':'>','value':'>'},
    {'label':'<','value':'<'},
    {'label':'>=','value':'>='},
    {'label':'<=','value':'<='},
    {'label':'btwn','value':'btwn'},
    {'label':'not','value':'not'},
]


class DtChooser(dash_table.DataTable):
    '''
    Create a Dash DataTable that displays filter options for the columns 
      of another DataFrame's data, that is located in a dcc.Store
    '''
    def __init__(self,dt_chooser_id,
                 main_store,
                 initial_columns_store=None,
                 initial_columns_store_colname='colname',
                 num_filters_rows=4,logger=None):
        '''
        
        :param dt_chooser_id: DataTable id
        :param store_of_df_data: dcc.Store object that holds the dict of the DataFrame that will be filtered
        :param logger: instance of logging, usually obtained from init_root_logger
        '''
        self.main_store = main_store
        self.initial_column_store = initial_columns_store
        self.initial_columns_store_colname = initial_columns_store_colname
        if self.initial_column_store is None:
            self.initial_column_store = dcc.Store(id=f"{dt_chooser_id}_initial_column_store",
                                                  data={})
        
        self.num_filters_rows = num_filters_rows
        self.logger = init_root_logger() if logger is None else logger
        
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
        
        self.selected_columns_dd = ColumnSelector(
            f"{dt_chooser_id}_selected_columns_dd", 
            displayed_rows=num_filters_rows)

        self.full_div = html.Div(
            [self.selected_columns_dd,self],
            style={'display':'grid',
                   'grid-template-columns':'20% 80%',
                   'grid-template-rows':'1fr'}
            )

        
    def register_app(self,theapp):
        @theapp.callback(
            [Output(self.selected_columns_dd.id,'data'),
             Output(self.selected_columns_dd.id,'columns'),
             Output(self.selected_columns_dd.id,'selected_rows')],
#             [Input(self.main_store.id,'data')],
#             State(self.initial_column_store.id,'data')
            [Input(self.main_store.id,'data'),
            Input(self.initial_column_store.id,'data')]
            )
        def _get_cols(df,dict_initial_column_store):
            if df is None:
                raise PreventUpdate("DtChooser._update_column_dropdown_options: no input data for columns")
            if type(df)!=pd.DataFrame:
                df = pd.DataFrame(df)
            df_initial_column_store = pd.DataFrame(dict_initial_column_store)
            if len(df_initial_column_store)>0:
                initial_columns = df_initial_column_store[self.initial_columns_store_colname].values
                try:
                    df = df[initial_columns]
                except Exception as e:
                    self.logger.warn(f'DtChooser._get_cols Exception: {str(e)}')
                    self.logger.warn(f'DtChooser._get_cols initial_columns: {initial_columns}')
                    self.logger.warn(f'DtChooser._get_cols df: ')
                    self.looger.warn(df.to_string())
                    PreventUpdate(str(e))
            df_return = pd.DataFrame({'option':df.columns.values})
            data = df_return.to_dict('records')
            columns = [{'name':c,'id':c} for c in df_return.columns.values]
            selected_rows = df_return.index.values
            return data,columns,selected_rows
        
        @theapp.callback(
            [Output(self.id,'dropdown'),Output(self.id,'data')],
            [Input(self.selected_columns_dd.id,'data')],
            [State(self.selected_columns_dd.id,'selected_rows')]
        )
        def _update_filters(selected_columns_df_dict,selected_rows):
            if selected_columns_df_dict is None:
                raise PreventUpdate("DtChooser._update_column_dropdown_options: no input data for columns")
            df_cols = pd.DataFrame(selected_columns_df_dict).loc[selected_rows].sort_index()
#             print(df_cols)
            newcols = df_cols['option'].values 
            thisdd = {
                'operation': {'options': _query_operators_options},
                'column':{'options': [{'label': i, 'value': i} for i in newcols]}
            }
            blanks = ['' for _ in range(self.num_filters_rows)]
            df_filter = pd.DataFrame({'column':blanks,'operation':blanks,'operator':blanks})
            return [thisdd,df_filter.to_dict('records')]
        return _update_filters


class DdFilterDiv(html.Div):
    def __init__(self,dd_filter_div_id,
                 columns_to_filter=None, 
                 default_column_to_filter_value=None):
        column_dd_options = []
        if columns_to_filter is not None:
            column_dd_options = [{'label':c,'value':c} for c in columns_to_filter]
        column_dd  = dcc.Dropdown(
            id=f'{dd_filter_div_id}_column_dd',
            options=column_dd_options,
            value=default_column_to_filter_value,      
            )
        operation_dd = dcc.Dropdown(
            id=f'{dd_filter_div_id}_operation_dd',
            options=_query_operators_options,
            )
        operator = dcc.Input(
            id=f'{dd_filter_div_id}_inp',
            debounce=True,
            style={'overflow':'auto'}
            )
        super(DdFilterDiv,self).__init__(
            [column_dd,operation_dd,operator],
            style={
                'grid-template-columns':'1fr',
                'display':'grid','grid-template-rows':'1fr 1fr 1fr',
                }
            )
        self.column_dd = column_dd
        self.operation_dd = operation_dd
        self.operator = operator


class DdChooser(html.Div):
    '''
    Create a html.Div containing a series of sub divs that allow the
      user to choose columns of a DataFrame using dropdown filters
    '''
    def __init__(self,dd_chooser_id,
                 main_store,num_filters=4,
                 columns_to_filter=None, 
                 default_column_to_filter_value=None,
                 logger=None):
        '''
        
        :param dd_chooser_id: id of main div, and the prefix used for sub-divs and components
        :param store_of_df_data: dcc.Store object that holds the dict of the DataFrame that will be filtered
        :param number of filters to display
        :param logger: instance of logging, usually obtained from init_root_logger
        '''
        self.logger= logger
        self.main_store = main_store
        self.query_store = dcc.Store(id=f'{dd_chooser_id}_query_store')
        self.num_filters = num_filters
        self.columns_to_filter = columns_to_filter
        self.default_column_to_filter_value = default_column_to_filter_value
        self.dd_filters = []
        for i in range(num_filters):
            dfd = DdFilterDiv(f'{dd_chooser_id}_dd_filter_{i}',
                              columns_to_filter=columns_to_filter,
                              default_column_to_filter_value=default_column_to_filter_value
                              )
            self.dd_filters.append(dfd)
        
        self.logger = init_root_logger() if logger is None else logger
        
        self.dd_filter_outputs = [Output(dd_filter.column_dd.id,'options') for dd_filter in self.dd_filters]
        
        super(DdChooser,self).__init__(
            self.dd_filters+[self.query_store],
            id=dd_chooser_id,
            style={'display':'grid',
                   'grid-template-columns':' '.join(['1fr' for _ in range(len(self.dd_filters))]),
                   'grid-template-rows':'1fr'}
        )
        
    def register_app(self,theapp):
        if self.columns_to_filter is None:
            @theapp.callback(
                self.dd_filter_outputs,
                [Input(self.main_store.id,'data')]
                )
            def _get_cols(df):
                '''
                Return a list of options arrays that will populate the column dropdown of each
                DdFilterDiv in  self.dd_filters
                :param df: Source of data with columns to be filtered
                '''
                if df is None:
                    raise PreventUpdate("DdChooser._get_cols: no input data for columns")
                if type(df)!=pd.DataFrame:
                    df = pd.DataFrame(df)
                options = [{'label':c,'value':c} for c in df.columns.values]
                return [options for _ in range(len(self.dd_filters))]
        
        @theapp.callback(
            Output(self.query_store.id,'data'),
            [Input(dd_filter.operator.id,'value') for dd_filter in self.dd_filters],
            [State(dd_filter.column_dd.id,'value') for dd_filter in self.dd_filters]+
            [State(dd_filter.operation_dd.id,'value') for dd_filter in self.dd_filters]
            )
        def _populate_query_store(*arg):
            query_operators = []
            query_columns = []
            query_operations = []
            for i in range(self.num_filters):
                query_operators.append(arg[i])
                query_columns.append(arg[i+self.num_filters])
                query_operations.append(arg[i+2*self.num_filters])
            if all([o is None for o in query_operators]):
                msg = 'DdChooser._populate_query_store: no data'
                self.logger.debug(msg)
                raise PreventUpdate(msg)
            df_query = pd.DataFrame({'column':query_columns,'operation':query_operations,'operator':query_operators})
            self.logger.debug(f'DdChooser._populate_query_store {self.id}: {df_query}')
            return df_query.to_dict('records')
        
class ColumnInfo(html.Div):
    def _mkid(self,s):
        return f"{self.main_id}_{s}"        
    
    def _do_eval(self,code,cols):
        '''
        Evaluate the lines in the argument code against every column in cols.
        The argument code is a multi-line string delimited with '\n' characters.
        :param code:
        :param cols:
        '''
        arr = []
        for col in cols:
            val = None
            try:
                for expr in code.split('\n'):
                    if len(expr)>0:
                        val = eval(expr)
            except Exception as e:
                val = str(e)
            arr.append(val)
        return arr
    
    def _eval_df(self,dd,cols):
        ddd = {k:self._do_eval(dd[k],cols) for k in dd.keys()}
        df = pd.DataFrame(ddd)
        return df

    def __init__(self,app,main_id,
            column_data_comp,column_key='colname',num_rows_input=8,
            logger=None,button_style=None
            ):
        '''
        
        :param app:
        :param main_id:
        :param column_data_comp: A component that holds the main data, whose columns 
                                  will be analyzed by ColumnInfo
        :param column_key: The name of the column for the DataFrame that holds all of the column titles
        :param num_rows_input: The number of "info" columns that the user wants to extract from each
                                 column of the original data in column_data_comp
        :param logger:
        :param button_style:
        '''
        # build pairs of dcc.Input components to define the output columns of ColumnInfo DataFrame
        self.main_id = main_id
        self.app = app
        self.num_rows_input = num_rows_input
        self.logger = logger
        if self.logger is None:
            self.logger = init_root_logger()
        self.column_data_comp = column_data_comp
        self.column_key = column_key
        self.column_info_dt = _make_dt(
            self._mkid('column_info_dt'), pd.DataFrame())
        
        class _InputPair(html.Div):
            def __init__(self,i,outer_class):
                self.inp_col_name = dcc.Input(id=outer_class._mkid(f'input_pair_col_name_{i}'),debounce=True)
                self.inp_expr = dcc.Textarea(id=outer_class._mkid(f'input_pair_expr_{i}'))
                super(_InputPair,self).__init__(
                    [self.inp_col_name,self.inp_expr],
                    style={
                        'display':'grid',
                        'grid-template-rows':'1fr 4fr',
                        'grid-template-columns':'1fr'
                        }
                    )
        self.col_info_btn = html.Button(
            id=self._mkid('col_info_btn'),
            children= "CREATE COL INFO",
            style=button_style
            )
        
        input_pairs = [_InputPair(i,self) for i in range(num_rows_input)]
        input_pairs_row_div = html.Div(input_pairs,
            style = {
                    'display':'grid',
                    'grid-template-columns': ' '.join(['1fr' for _ in range(num_rows_input)]),
                    'grid-template-rows':'1fr'
                }
                                       )
        input_pairs_div = html.Div(
            [self.col_info_btn,input_pairs_row_div],
            style = {
                    'display':'grid',
                    'grid-template-columns': '10% 90%',
                    'grid-template-rows':'1fr'
                }
            )

        children = [input_pairs_div,self.column_info_dt]
        # call the super constructor
        super(ColumnInfo,self).__init__(children,id=self._mkid('column_info_div'))
        
        col_name_inputs = [State(ip.inp_col_name.id,'value') for ip in input_pairs]
        col_inp_exp_inputs = [State(ip.inp_expr.id,'value') for ip in input_pairs]
        
        self.col_states =  col_name_inputs + col_inp_exp_inputs 

    def register_app(self,theapp):
        @theapp.callback(
            [
                Output(self.column_info_dt.id,'data'),
                Output(self.column_info_dt.id,'columns'),
                ],
            [
                Input(self.col_info_btn.id,'n_clicks'),
                Input(self.column_data_comp.id,'data')
                ],
            self.col_states             
            )
        def _build_dt(*inputs):
            '''
            Build a data table, where each column is a regex extracted phrase in the column of the orginal data,
              like word "padd" from data whose columns were about EIA price/storage 
            info from energy "padd" areas in the U.S.
            
            @param inputs: 
                    1. n_clicks: from the col_info_btn
                    2. data: from column_data_comp
                    3 .. num_rows_input:  all of colname dcc.Input values
                    3+num_rows_input .. end: all of the colvalue dcc.TextArea values that hold code used to 
                                              extract
            '''
            # Ignore the first input, which is the n_clicks return from the button
            n_clicks = inputs[0]
            # The next input (inputs[1]) holds column_data_comp data
            #   Use it get the original data, whose columns you wish to analyze
            column_data_comp_data = inputs[1]
            if column_data_comp_data is None:
                raise PreventUpdate("ColumnInfo._build_dt no data")
            # Put the original columns into the rows of the DataFrame df_columns
            if type(column_data_comp_data) == pd.DataFrame:
                df_columns =  column_data_comp_data
            else:
                df_columns = pd.DataFrame(column_data_comp_data)
            self.logger.info(f'Columninfo._build_dt: {self.column_key}')
            
            try:
                cols = df_columns[self.column_key]
            except Exception as e:
                msg = f'Columninfo._build_dt: {str(e)}'
                self.logger.warn(msg)
                raise PreventUpdate(msg)
            
            # Now build the col_info dataframe, which will build "information" regex phrases that
            #   allow you to group the original columns.
            col_builder_inputs = {}
            
            # The inputs from 2 to the end (inputs[2:]) hold the col_info colnames and textareas with python code
            col_states_arr = inputs[2:]
            for i in range(self.num_rows_input):
                col_name = col_states_arr[i]
                expr_text =  col_states_arr[i+self.num_rows_input]
                if (col_name is  None) or (len(col_name)<=0):
                    continue
                if (expr_text is  None) or (len(expr_text)<=0):
                    continue
                col_builder_inputs[col_name] = expr_text
            dict_column_info_df = self._eval_df(col_builder_inputs,cols)
            df_return = pd.DataFrame(dict_column_info_df)
            ret_columns = [{'name':c,'id':c} for c in df_return.columns.values]
            return df_return.to_dict('records'),ret_columns
            

class FeatureSelector(html.Div):
    def _mkid(self,s):
        return f"{self.main_id}_{s}"        
    
    def __init__(
            self,app,main_id,
            main_store,num_rows_input=None,
            column_info_colname='colname',
            logger=None,button_style=None
            ):
        self.main_id = main_id
        self.save_store = dcc.Store(id=self._mkid('save_store'))
        self.app = app
        self.logger = logger
        if self.logger is None:
            self.logger = init_root_logger()
        self.main_store = main_store
        self.column_info_colname = column_info_colname
        self.column_store = dcc.Store(id=self._mkid('column_store'))

        if num_rows_input is None:
            num_rows_store = dcc.Store(id=self._mkid("num_rows_store"),data=ROWS_FOR_DASHTABLE)
            self.num_rows_input = Input(num_rows_store.id,"data")
        else:
            self.num_rows_input = num_rows_input
        
        self.filter_btn = html.Button(
            id=self._mkid('filter_button'),
            children= "RUN FILTER",
            style=button_style
            )
        
        self.ddc = DdChooser(
            self._mkid('dd_chooser'), 
            self.column_store, 
            num_filters=4, 
            columns_to_filter=[self.column_info_colname], 
            default_column_to_filter_value=self.column_info_colname,
            logger=self.logger)
        
        self.ddc_or = DdChooser(
            self._mkid('dd_chooser_or'), 
            self.column_store, 
            num_filters=4, 
            columns_to_filter=[self.column_info_colname], 
            default_column_to_filter_value=self.column_info_colname,
            logger=self.logger)
        

        filter_grid = html.Div(
            [self.filter_btn,self.ddc,self.ddc_or],
            style={'display':'grid','grid-template-columns':'10% 45% 45%',
                   'grid-template-rows':'1fr'}
            )

        self.dt_data = _make_dt(
            self._mkid('dt_data'),pd.DataFrame(),
            displayed_rows=ROWS_FOR_DASHTABLE,page_action='custom'
        )

        self.dt_data_div = html.Div([self.dt_data],self._mkid('dt_data_div'))
        self.column_info = ColumnInfo(app, self._mkid('column_info'), 
                                      self.dt_data, column_key='colname', logger=self.logger)
        
        self.column_info_unique_dt = _make_dt(
            self._mkid('column_info_unique_dt'),pd.DataFrame(),
            displayed_rows=ROWS_FOR_DASHTABLE,page_action='native'
        )
        
        self.column_category_suggestions_dt = _make_dt(
            self._mkid('column_category_suggestions'),pd.DataFrame(),
            displayed_rows=ROWS_FOR_DASHTABLE,page_action='native'
        )
        
        self.column_category_suggestions_btn = html.Button(
            'Build Column Category Suggestions',
            id=self._mkid('column_category_suggestions_btn'))
        self.column_category_suggestions_div = html.Div(
            [
                self.column_category_suggestions_btn,
                dcc.Loading(children=[self.column_category_suggestions_dt])
             ]
            )
                
        t1 = dcc.Tab(children=self.dt_data_div,
                        label='Column Data',value='raw_data')
        t2 = dcc.Tab(children=self.column_info,
                        label='Build Column Categories',value='column_info')
        t3 = dcc.Tab(children=self.column_info_unique_dt,
                        label='Unique Column Categories',value='column_info_unique_dt')
        t4 = dcc.Tab(children=self.column_category_suggestions_div,
                        label='Suggested Column Categories',value='column_category_suggestions_div')
        
        datatable_div = dcc.Tabs(
            children=[t1,t2,t3,t4,self.column_store], value='raw_data')
        children =  html.Div([
                            filter_grid,
                            dcc.Loading(
                                children=datatable_div, 
                                fullscreen=True, type="dot"
                                ),
                        ],            
                    )
           
        super(FeatureSelector,self).__init__(children,id=self._mkid('feature_viewer_div'))
        self.logger.info('FeatureSelector.__init__ done')

    
        
    def register_app(self,theapp):
        @theapp.callback(
            Output(self.save_store.id,'data'),
            Input(self.filter_btn.id,'n_clicks'),
            [
                State(self.column_info.column_info_dt.id,'data'),
                State(self.ddc.query_store.id,'data'),
                State(self.ddc_or.query_store.id,'data'),
                ]
            )
        def _update_save(
                _,
                column_info_dict,
                dtc_query_dict_df,
                dtc_or_query_dict_df
                ):
            return {
                'column_info':column_info_dict,
                'ddc':dtc_query_dict_df,
                'dtc_or':dtc_or_query_dict_df
                }
        
        @theapp.callback(
            Output(self.column_store.id,'data'),
            Input(self.main_store.id,'data')
            )
        def _populate_column_store(df_main):
            if df_main is None or (len(df_main)<1):
                raise PreventUpdate('FeatureSelector._populate_column_store: no main data')
            main_cols = df_main.columns.values
            df_return = pd.DataFrame(
                {'colnum':list(range(len(main_cols))),
                 self.column_info_colname:main_cols
                 }
                )
            return df_return.to_dict('records')
            
        @theapp.callback(
            [Output(self.dt_data.id,'page_size')],
            [self.num_rows_input]
        )
        def _update_page_size(value):
#             print(f"_update_page_size: {value}")
            return value
                
        @theapp.callback(
            [
                Output(self.dt_data.id,'data'),
                Output(self.dt_data.id,'columns'),
                Output(self.dt_data.id,'page_count'),
                Output(self.column_info_unique_dt.id,'data'),
                Output(self.column_info_unique_dt.id,'columns'),
                ], 
            [
                Input(self.column_store.id, "data"),
                Input(self.dt_data.id,'page_current'),
                Input(self.filter_btn.id,'n_clicks'),
                Input(self.dt_data.id,'sort_by'),
                Input(self.column_info.column_info_dt.id,'data')
                ],
            [
                State(self.dt_data.id,'page_size'),
                State(self.ddc.query_store.id,'data'),
                State(self.ddc_or.query_store.id,'data'),
                ]
            )
        def display_df(
                dict_df, 
                page_current,
                n_clicks,
                sort_by,
                column_info_dict,
                page_size,
                dtc_query_dict_df,
                dtc_or_query_dict_df
                ):            
            self.logger.debug(f"entering FeatureSelector.display_df:{n_clicks}")
            if dict_df is None:
                raise PreventUpdate('FeatureSelector.display_df callback: no data')
            
            # Get main data which has the columns that you want to analyze
            df = pd.DataFrame(dict_df)
            self.logger.debug(f"entering FeatureSelector.display_df: {datetime.datetime.now()} {len(df)} {page_size}")
            pagcur = int(str(page_current))
            if (pagcur is None) or (pagcur<0):
                pagcur = 0
            ps = ROWS_FOR_DASHTABLE
            
            if (page_size is not None):
                ps = int(str(page_size))
            
            df_after_filter = df
            # execute first filters
            self.logger.debug(f'FeatureSelector.display_df dtc_query_dict_df: {dtc_query_dict_df}')
            if (dtc_query_dict_df is not None):
                df_dtc = pd.DataFrame(dtc_query_dict_df)
                if len(df_dtc)>0:
                    df_after_filter = execute_filter_query(
                        df,df_dtc
                        )
            
            if len(sort_by):
                df_after_filter = df_after_filter.sort_values(
                    [col['column_id'] for col in sort_by],
                    ascending=[
                        col['direction'] == 'asc'
                        for col in sort_by
                    ],
                    inplace=False
                )
            
            # execute "OR" filters
            self.logger.debug(f'FeatureSelector.display_df dtc_or_query_dict_df: {dtc_or_query_dict_df}')
            if (dtc_or_query_dict_df is not None):
                df_dtc_or = pd.DataFrame(dtc_or_query_dict_df)
                if len(df_dtc_or)>0:
                    df_or_filter = execute_filter_query(
                        df,df_dtc_or
                        )
                    df_after_filter = df_after_filter.append(df_or_filter)
                    
            if len(sort_by):
                df_after_filter = df_after_filter.sort_values(
                    [col['column_id'] for col in sort_by],
                    ascending=[
                        col['direction'] == 'asc'
                        for col in sort_by
                    ],
                    inplace=False
                )

            beg_row = pagcur*ps
            if pagcur*ps > len(df):
                beg_row = len(df) - ps
        
            dict_data = df_after_filter.iloc[beg_row:beg_row + ps].to_dict('records')
            cols = [{"name": i, "id": i} for i in df_after_filter.columns.values]
            page_count = int(len(df_after_filter)/ps) + (1 if len(df_after_filter) % ps > 0 else 0)

            # Populate column_info.column_unique_info_dt
            #   First re-create a DataFrame of the the column info, with columns for different categories of columns
            df_column_info_dict = pd.DataFrame(column_info_dict)
            df_column_info_dict_unique = create_unique_values(df_column_info_dict)
            dict_column_info_dict_unique = df_column_info_dict_unique.to_dict('records')
            columns_column_info_dict_unique = [{'label':c, 'id':c} for c in df_column_info_dict_unique.columns.values]

            return dict_data,cols,page_count,dict_column_info_dict_unique,columns_column_info_dict_unique
        
        @theapp.callback(
            [
                Output(self.column_category_suggestions_dt.id,'data'),
                Output(self.column_category_suggestions_dt.id,'columns'),
                ],
            Input(self.column_category_suggestions_btn.id,'n_clicks'),
#             State(self.column_store.id,'data')
            State(self.dt_data.id,'data'),
            )
        def _create_column_category_suggestions(_,dict_dt_data):
            if dict_dt_data is None:
                raise PreventUpdate('FeatureSelector._create_column_category_suggestions callback: no data')
            df = pd.DataFrame(dict_dt_data) 
            df_cols = df[[self.column_info_colname]]
            df_doc  = create_nlp_doc(df_cols)
            df_doc2 = df_doc[df_doc.ptext.str.split(' ').str.len()<=3]
            df_doc3 = df_doc2[~df_doc2.ptext.str.contains('^[0-9]')].sort_values('prank',ascending=False)
            columns = [{'label':c, 'id':c} for c in df_doc3.columns.values]
            dict_df3 = df_doc3.to_dict('records')
            return dict_df3,columns
        
        
        self.ddc.register_app(theapp)
        self.ddc_or.register_app(theapp)
        self.column_info.register_app(theapp)

class CsvViewer(html.Div):
    def __init__(self,app,main_id,
                 main_store,
                 initial_columns_store=None,
                 initial_columns_store_colname='colname',
                 num_rows_input=None,
                 max_displable_cols=20,
                 logger=None,button_style=None):
        self.main_id = main_id
        self.app = app
        self.max_displable_cols = max_displable_cols
        if num_rows_input is None:
            num_rows_store = dcc.Store(id=self._mkid("num_rows_store"),data=ROWS_FOR_DASHTABLE)
            self.num_rows_input = Input(num_rows_store.id,"data")
        else:
            self.num_rows_input = num_rows_input
            
        self.main_store = main_store
        self.logger = init_root_logger() if logger  is None else logger
        
        self.filter_btn = html.Button(
            id=self._mkid('filter_button'),
            children= "RUN FILTER",
            style=button_style
            )
        
        self.dtc = DtChooser(
            self._mkid('dtc'),
            self.main_store,
            initial_columns_store=initial_columns_store,
            initial_columns_store_colname=initial_columns_store_colname,
            num_filters_rows=4,
            logger=self.logger)
        
#         self.dt_data = self._make_dt(
        self.dt_data = _make_dt(
            self._mkid('dt_data'),pd.DataFrame(),
            displayed_rows=ROWS_FOR_DASHTABLE,page_action='custom'
        )

        self.dt_data_div = html.Div([self.dt_data],self._mkid('dt_data_div'))
        
        filter_grid = html.Div(
            [self.filter_btn,self.dtc.full_div],
            style={'display':'grid','grid-template-columns':'10% 90%',
                   'grid-template-rows':'1fr'}
            )
                
        self.unique_div = html.Div(id=self._mkid('unique_div'))
        self.agg_div = html.Div(id=self._mkid('agg_div'))
        
        
#         graph_title_row = dashapp.make_page_title("XY Graph",div_id=self._mkid('graph_title_row'),html_container=html.H3)
        self.graph1_div = XyGraphDefinition(self.dt_data, self._mkid('graph1'))
        
        
        
        t1 = dcc.Tab(children=self.dt_data_div,
                        label='Raw Data',value='raw_data')
        t2 = dcc.Tab(children=self.unique_div,
                        label='Unique Data',value='unique_data')
        t3 = dcc.Tab(children=self.agg_div,
                        label='Aggregate Data',value='agg_data')
        t4 = dcc.Tab(children=self.graph1_div,
                        label='Graph1',value='graph1')
        
        datatable_div = dcc.Tabs(
            children=[t1,t2,t3,t4], value='raw_data')
        children =  html.Div([
                            filter_grid,
                            dcc.Loading(
                                children=datatable_div, 
                                fullscreen=True, type="dot"
                                ),
                        ],            
                    )
           
        super(CsvViewer,self).__init__(children,id=self._mkid('csv_viewer_div'))
        self.logger.info('CsvViewer.__init__ done')
        
    def _mkid(self,s):
        return f"{self.main_id}_{s}"        


    def register_app(self,theapp):
        @theapp.callback(
            [Output(self.dt_data.id,'page_size')],
            [self.num_rows_input]
        )
        def _update_page_size(value):
            self.logger.debug(f"_update_page_size: {value}")
            return value
                
        @theapp.callback(
            [
                Output(self.dt_data.id,'data'),
                Output(self.dt_data.id,'columns'),
                Output(self.dt_data.id,'page_count'),
                Output(self.unique_div.id,'children'),
                Output(self.agg_div.id,'children')
                ], 
            [
                Input(self.main_store.id, "data"),
                Input(self.dt_data.id,'page_current'),
                Input(self.filter_btn.id,'n_clicks'),
                Input(self.dt_data.id,'sort_by'),
                Input(self.dtc.selected_columns_dd.id,'data'),
                Input(self.dtc.selected_columns_dd.id,'selected_rows')
                ],
            [
                State(self.dt_data.id,'page_size'),
                State(self.dtc.id,'data'),
#                 State(self.dtc.selected_columns_dd.id,'data'),
#                 State(self.dtc.selected_columns_dd.id,'selected_rows')
                ]
            )
#         def display_df(df, page_current,_,sort_by,page_size,dtc_query_dict_df,
#                        selected_columns_dd_data,selected_columns_dd_selected_rows):
        def display_df(df, page_current,_,sort_by,
                       selected_columns_dd_data,selected_columns_dd_selected_rows,
                       page_size,dtc_query_dict_df):
            if df is None:
                raise PreventUpdate('CsvViewer.display_df callback: no data')
            self.logger.debug(f"entering display_df: {datetime.datetime.now()} {len(df)} {page_size}")
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
                    df_after_filter = execute_filter_query(
                        df,df_dtc
                        )

            if (selected_columns_dd_data is not None) and (len(selected_columns_dd_data)>0):
                cols_to_show = pd.DataFrame(selected_columns_dd_data).loc[selected_columns_dd_selected_rows].sort_index()['option'].values
                df_after_filter = df_after_filter[cols_to_show]                   
            
            # only allow max columns
            df_after_filter = df_after_filter[df_after_filter.columns.values[:self.max_displable_cols]]
            if len(sort_by):
                df_after_filter = df_after_filter.sort_values(
                    [col['column_id'] for col in sort_by],
                    ascending=[
                        col['direction'] == 'asc'
                        for col in sort_by
                    ],
                    inplace=False
                )
            
            beg_row = pagcur*ps
            if pagcur*ps > len(df):
                beg_row = len(df) - ps
        
            dict_data = df_after_filter.iloc[beg_row:beg_row + ps].to_dict('records')
            cols = [{"name": i, "id": i} for i in df_after_filter.columns.values]
            page_count = int(len(df_after_filter)/ps) + (1 if len(df_after_filter) % ps > 0 else 0)
            
            # create df_unique
            df_unique =  create_unique_values(df_after_filter)
            dt_unique_div = html.Div([_make_dt(self._mkid('dt_unique'),df_unique)])

            df_agg = create_aggregate_summary(df_after_filter)
            dt_agg_div = html.Div([_make_dt(self._mkid('dt_agg'),df_agg)])
            return dict_data,cols,page_count,dt_unique_div,dt_agg_div
        
        self.dtc.register_app(theapp)
        self.graph1_div.register_app(theapp)

class FiledownloadComponent(html.Div):
    def _mkid(self,s):
        return f"{self.main_id}_{s}"        
    
    def __init__(self,main_id,route_url,logger=None):
        self.main_id = main_id
        self.route_url = route_url
        self.logger = init_root_logger() if logger is None else logger
        # create important id's
        
        # create callback that populates the A link
        def _update_link(input_value):
            v = input_value[0]
            if v is None:
                v = self.dropdown_values[0]
            return ['/dash/urlToDownload?value={}'.format(v)]        
        self.filename_input = dcc.Input(
            id=self._mkid('filename_input'),
            debounce=True
            )
        self.href_comp = html.A("Save Config",href='',id=self._mkid('href_comp'))
        super(FiledownloadComponent,self).__init__()
    
    def register_app(self,theapp):
        @theapp.server.route(self.route_url)
        def download_csv():
            value = flask.request.args.get('value')            
            fn = self.route_url
            print(f'FileDownLoadDiv callback file name = {fn}')
            return flask.send_file(fn,
                               mimetype='json',
                               attachment_filename=fn,
                               as_attachment=True)
        @theapp.callback(
            Output(self.href_comp.id,'href'),
            Input(self.filename_input.id,'value')
            )
        def _update_filename():
            pass
