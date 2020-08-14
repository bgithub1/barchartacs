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
import pandas as pd
from dashapp import dashapp2 as dashapp#@UnResolvedImport
import progressive_dropdown as progdd#@UnResolvedImport

args = sys.argv

configs = open('./temp_folder/dashapp_db_table_config_name.txt','r').read().split(',')
config_name=configs[0]

app_port = 8812
if len(configs)>1:
    app_port=int(configs[1])
    
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


def create_unique_values(df):
    vals = [(c,list(df[c].unique())) for c in df.dtypes[(df.dtypes=='object') | (df.dtypes=='str')].index]
    if len(vals)<1:
        return pd.DataFrame()
    df_unique = pd.DataFrame({vals[0][0]:vals[0][1]})
    for v in vals[1:]:
        df_unique = pd.concat([df_unique,pd.DataFrame({v[0]:v[1]})],axis=1)
    return df_unique

def create_aggregate_summary(df,rounding=4):
    cols = df.columns.values
    num_cols = [c for c in cols if (df[c].dtype=='float64') or (df[c].dtype=='int64')]
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



class XyGraphDefinition(html.Div):
    def __init__(self,data_store,div_id,num_graph_filter_rows=2,
                 logger=None):
        titles = ['X Column','Y Left Axis','Y Right Axis']
#         prog_dd_divs,prog_dd_links,prog_dds = progressive_dropdowns(
#             data_store,f'{div_id}_dropdowns',len(titles),title_list=titles)
        self.prog_dd = progdd.ProgressiveDropdown(
            data_store,f'{div_id}_dropdowns',
            len(titles),title_list=titles
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
        
#         filter_rows = prog_dd_divs + [y_left_axis_div,y_right_axis_div] + [graph_button_div,graph_title_div]
        filter_rows = self.prog_dd.pd_div_list + [y_left_axis_div,y_right_axis_div] + [self.graph_button_div,graph_title_div]
        fr1 = dashapp.multi_column_panel(filter_rows[0:3])
        fr2 = dashapp.multi_column_panel(filter_rows[3:6])
        fr3 = dashapp.multi_column_panel(filter_rows[6:7])
        filter_div =dashapp.multi_row_panel([fr1,fr2,fr3])
#         self.filter_rows = filter_rows
#         self.filter_div = filter_div
#         self.div_id = div_id
#         self.graph = self.graph_comp
        super(XyGraphDefinition,self).__init__(
            [filter_div,self.graph_comp],id=div_id
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
#             x_col = input_data[1]
            if type(x_col) == list:
                x_col = x_col[0]
#             y_left_cols = input_data[2]
#             y_right_cols = input_data[3]
            if any([x_col is None,y_left_cols is None]):
                raise PreventUpdate("No columns selected for graph")
                
#             if input_data[4] is None:
#                 dict_df = []
#             else:
#                 dict_df = list(input_data[4].values())[0]
            df = pd.DataFrame(dict_df)
#             y_left_label = input_data[5]
#             y_right_label = input_data[6]
#             graph_title = input_data[7]
            
            yrc = [] if (y_right_cols is None) or (y_right_cols[0] is None) else y_right_cols
            df = df[[x_col] + y_left_cols + yrc]
            fig = dashapp.plotly_plot(
                df_in=df,x_column=x_col,yaxis2_cols=y_right_cols,
                y_left_label=y_left_label,y_right_label=y_right_label,
                plot_title=graph_title)
            return fig
        self.prog_dd.register_app(theapp)
#         dlink = dashapp.DashLink(inputs,outputs,_build_fig_callback,states)

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
    
    def __init__(self,app,main_id,zip_or_csv='zip'):
        self.main_id = main_id
        self.app = app
        self.zip_or_csv = zip_or_csv
        self.btn_run = html.Button(
            children=f"Upload {zip_or_csv.upper()}",id=self._mkid('btn_run'),
            )
        self.num_rows_input = dcc.Input(
            id=self._mkid('num_rows_input'),debounce=True,
            value=ROWS_FOR_DASHTABLE,type='number',
            min=100, max=ROWS_FOR_DASHTABLE*2, step=100,
            )
        # create uploader
        uploader_text = make_text_centered_div(f"Choose a {zip_or_csv.upper()} File")
        self.uploader_comp = dcc.Upload(
                    id=self._mkid("uploader_comp"),
                    children=uploader_text,
                    accept = f'.{zip_or_csv.lower()},.{zip_or_csv.upper()}' ,
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
                df = pd.DataFrame(table_data_dict)
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
                 main_store,num_filters_rows=4,logger=None):
        '''
        
        :param dt_chooser_id: DataTable id
        :param store_of_df_data: dcc.Store object that holds the dict of the DataFrame that will be filtered
        :param selected_columns_dd: a dcc.Dropdown that holds specifically selected columns by the user to show
        :param logger: instance of logging, usually obtained from init_root_logger
        '''
        self.main_store = main_store
        
#         self.selected_columns_dd = dcc.Dropdown(
#             id=f'{dt_chooser_id}_selected_columns_dd',
#             multi=True)
        
        
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
        
        self.selected_columns_dd = ColumnSelector(
            f"{dt_chooser_id}_selected_columns_dd", 
#             options_input_dashtable=self.id, 
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
            [Input(self.main_store.id,'data')]
            )
        def _get_cols(df):
            if df is None:
                raise PreventUpdate("DtChooser._update_column_dropdown_options: no input data for columns")
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
            [self.filter_btn,self.dtc.full_div],
            style={'display':'grid','grid-template-columns':'10% 90%',
                   'grid-template-rows':'1fr'}
            )
                
        self.unique_div = html.Div(id=self._mkid('unique_div'))
        self.agg_div = html.Div(id=self._mkid('agg_div'))
        
        
        graph_title_row = dashapp.make_page_title("XY Graph",div_id=self._mkid('graph_title_row'),html_container=html.H3)
        self.graph1_div = XyGraphDefinition(self.dt_data, self._mkid('graph1'))
#         self.graph1_div = html.Div([self.graph1_def.filter_div,graph_title_row,self.graph1_def.graph])
        
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
        print('CsvViewer.__init__ done')
        
    def _mkid(self,s):
        return f"{self.main_id}_{s}"        

    def _make_dt(self,dt_id,df,displayed_rows=100,page_action='native'):
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
            } ,
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
                Input(self.dt_data.id,'sort_by')
                ],
            [
                State(self.dt_data.id,'page_size'),
                State(self.dtc.id,'data'),
                State(self.dtc.selected_columns_dd.id,'data'),
                State(self.dtc.selected_columns_dd.id,'selected_rows')
                ]
            )
        def display_df(df, page_current,_,sort_by,page_size,dtc_query_dict_df,
                       selected_columns_dd_data,selected_columns_dd_selected_rows):
            if df is None:
                raise PreventUpdate('CsvViewer.display_df callback: no data')
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
#                     if (cols_to_show is not None) and (len(cols_to_show)>0):
#                         df_after_filter = df_after_filter[cols_to_show]                   
                    if (selected_columns_dd_data is not None) and (len(selected_columns_dd_data)>0):
                        cols_to_show = pd.DataFrame(selected_columns_dd_data).loc[selected_columns_dd_selected_rows].sort_index()['option'].values
                        df_after_filter = df_after_filter[cols_to_show]                   
            
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
            dt_unique_div = html.Div([self._make_dt(self._mkid('dt_unique'),df_unique)])

            df_agg = create_aggregate_summary(df_after_filter)
            dt_agg_div = html.Div([self._make_dt(self._mkid('dt_agg'),df_agg)])
            return dict_data,cols,page_count,dt_unique_div,dt_agg_div
        
        dtc_callback = self.dtc.register_app(theapp)
        self.graph1_div.register_app(theapp)
#         xygraph_dashlinks = self.graph1_def.dashlinks
#         for xygraph_dashlink in xygraph_dashlinks:
#             xygraph_dashlink.callback(theapp)
#         return _update_page_size,display_df,dtc_callback


