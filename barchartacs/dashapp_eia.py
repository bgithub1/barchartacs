'''
Created on Aug 19, 2020

@author: bperlman1
'''

import sys
import dash_table
import numpy as np
import pandas as pd
import re
import json

import dash_core_components as dcc
import dash_html_components as html
from dash_extensions.enrich import Dash, ServersideOutput, Output, Input, State, Trigger
from dash.exceptions import PreventUpdate
import logging
import progressive_dropdown as progdd#@UnResolvedImport
args = sys.argv


DEFAULT_LOG_PATH = './logfile.log'
DEFAULT_LOG_LEVEL = 'INFO'

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
        style_cell={
        'whiteSpace': 'normal',
        'height': 'auto',
        },
    )
    df = pd.DataFrame() if df_pet is None else df_pet
    dt.data=df.to_dict('records')
    dt.columns=[{"name": i, "id": i} for i in df.columns.values]                    
    return dt


def create_unique_values(df_col_ids):
    df = df_col_ids[[c for c in df_col_ids.columns.values if c!='col']]
    # create a list of tuples, where each tuple has a column name and a list of unique values
    vals = [(c,sorted(list(df[c].unique()))) for c in df.dtypes[(df.dtypes=='object') | (df.dtypes=='str')].index]
    vals = [c for c in vals if (c is not None) and (len(c)>0)]
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

def padd_name(col):
    val = "(padd [0-9][a-z]{0,1}){1,2}"
    val = re.split(val,col.lower())
    val = ''.join(val[1:-1])
    return val
    
def padd_location(col):
    val = "(padd [1-5][a-z]{0,1}){1,2}"
    val = re.split(val,col.lower())[0]
    return val

def padd_region(col):
    val = "(padd [0-9][a-z]{0,1}){1,2}"
    val = re.split(val,col.lower())
    val = val[0].strip()
    return val

def padd_production_type(col):
    val = "(padd [0-9][a-z]{0,1}){1,2}"
    val = re.split(val,col.lower())
    val = val[-1].strip().lower().split('of')[0]
    return val
    
def padd_source(col):
    val = "(padd [0-9][a-z]{0,1}){1,2}"
    val = re.split(val,col.lower())
    val = val[-1].strip().lower().split('of')[-1]    
    return val

def padd_fuel_type(col):
    val = "(padd [1-5][a-z]{0,1}){1,2}"
    val = re.split(val,col.lower())
    val = re.split('of',val[-1])
    val = val[1]
    return val
    

def gas_price_region(col):
    val = re.split('price[ ]',col)
    val = [v.rstrip(' ') for v in val]
    val = ' '.join(val[0].split(' ')[0:-1])
    val = re.split('(regular)|(premium)|(midgrade)|(all grades)',val)[0]    
    return val

def gas_price_gas_type(col):
    val = re.split('price[ ]',col)
    val = [v.rstrip(' ') for v in val]
    val = ' '.join(val[0].split(' ')[0:-1])
    val = re.split('(regular)|(premium)|(midgrade)|(all grades)',val)[-1]
    val = re.split('retail',val)[0]
    return val

def gas_price_grade(col):
    val =  re.findall('(regular)|(premium)|(midgrade)|(all grades)',col) 
    if len(val)<1:
        return ''
    val = val[0]
    if type(val)==tuple:
        val = ''.join(list(val))
    return val


def do_eval(code,cols):
    arr = []
    for col in cols:
        val = None
        try:
            val = code(col)
        except Exception as e:
#             val = str(e)
            val = ''
        arr.append(val.strip(' ').lstrip(' '))
    return arr

def eval_df(func_dict,cols):
    dict_df = {}
    for k in func_dict.keys():
        dict_df[k] = do_eval(func_dict[k],cols)
    df = pd.DataFrame(dict_df)
    return df

def get_search_columns(df_pet_lower,column_extractor_list,isin_list,notin_list):
    column_extractor_dict = {}
    for m in column_extractor_list:
        column_extractor_dict[m.__name__] = m


    cols_no_date = [c for c in df_pet_lower.columns.values if c!='date']
    isin_regex = lambda c,TF,l:all([(len(re.findall(r,c))>0)==TF for r in l])
    cols_no_date = [c for c in cols_no_date if isin_regex(c,True,isin_list)]

    cols_no_date = [c for c in cols_no_date if isin_regex(c,False,notin_list)]

    df_eval = eval_df(column_extractor_dict,cols_no_date)
    df_eval['col'] = cols_no_date
    return df_eval

def get_search_lists(df_pet_lower,column_extractor_list,isin_list,notin_list):
    df_eval = get_search_columns(df_pet_lower,column_extractor_list,isin_list,notin_list)
    df_unique = create_unique_values(df_eval)
    return df_unique



class PaddVolumeCategories():
    def __init__(self,df_pet_lower):
        column_extractor_list = [padd_name,padd_location,padd_production_type,padd_fuel_type]
        isin_list = ['(padd [1-5][a-z]{0,1}){1,2}','gasoline','of']
        notin_list = ['prices']
        self.df_cols = get_search_columns(df_pet_lower,column_extractor_list,isin_list,notin_list)
        self.df_categories = create_unique_values(self.df_cols)

class NonPaddGasPriceCategories():
    def __init__(self,df_pet_lower):
        column_extractor_list = [gas_price_region,gas_price_gas_type,gas_price_grade]
        isin_list = ['prices','gasoline','(regular)|(premium)|(midgrade)|(all grades)']
        notin_list = ['padd']
        self.df_cols = get_search_columns(df_pet_lower,column_extractor_list,isin_list,notin_list)
        self.df_categories = create_unique_values(self.df_cols)
        
class EiaCategories():
    def __init__(self,df_pet_lower):
        self.pvc = PaddVolumeCategories(df_pet_lower)
        self.npnc = NonPaddGasPriceCategories(df_pet_lower)
        df_all_categories = pd.concat((self.pvc.df_cols,self.npnc.df_cols)).fillna('')
        self.df_all_categories = create_unique_values(df_all_categories)
        df_all_cols = pd.DataFrame({'col':df_pet_lower.columns.values})
        df_all_cols = df_all_cols.merge(self.pvc.df_cols,on='col',how='left')
        self.df_all_cols = df_all_cols.merge(self.npnc.df_cols,on='col',how='left').fillna('')
    
    def get_column_set(self,
                       padd_name_list=None,
                       padd_location_list=None,
                       padd_production_type_list=None,
                       padd_fuel_type_list=None,
                       gas_price_region_list=None,
                       gas_price_gas_type_list=None,
                       gas_price_grade_list=None
                       ):
        all_true = pd.Series([True for _ in range(len(self.df_all_cols))])
        def _make_condition(col,col_list):
            cond = all_true if ((col_list is None) or (len(col_list)<1)) else self.df_all_cols[col.__name__].isin(col_list)
            return cond
        
        c_padd_name = _make_condition(padd_name,padd_name_list)
        c_padd_location =  _make_condition(padd_location,padd_location_list)
        c_padd_production_type =  _make_condition(padd_production_type,padd_production_type_list)
        c_padd_fuel_type = _make_condition(padd_fuel_type,padd_fuel_type_list)
        
        c_gas_price_region = _make_condition(gas_price_region,gas_price_region_list)
        c_gas_price_gas_type = _make_condition(gas_price_gas_type,gas_price_gas_type_list )
        c_gas_price_grade = _make_condition(gas_price_grade,gas_price_grade_list)
        c_all = c_padd_name & c_padd_location & c_padd_production_type & c_padd_fuel_type & c_gas_price_region & c_gas_price_gas_type & c_gas_price_grade
        df_ret =  self.df_all_cols[c_all]
        return df_ret
    
class HtmlDiv(html.Div):
    def _mkid(self,s):
        return f"{self.main_id}_{s}"        
    def __init__(self,main_id,children,logger=None,**args):
        self.main_id = main_id
        super(HtmlDiv,self).__init__(children,id=self._mkid('main_div'),**args)
        self.logger = init_root_logger() if logger is None else logger

class EiaCategoriesDiv(HtmlDiv):
    def __init__(self,main_id,
                 input_pet_file='./temp_folder/df_pet_lower.csv',
                 eia_categories_default_json='./temp_folder/eia_categories_default_config.json'
                 ):
        super(EiaCategoriesDiv,self).__init__(main_id,None)
        # get default dropdown values
        default_categories_dict = {}
        try:
            f = open(eia_categories_default_json,'r')
            default_categories_dict = json.load(f)
        except Exception as e:
            self.logger.warn(f'EiaCategoriesDiv json.loads(eia_categories_default_json): {str(e)}')
        df_pet_lower = pd.read_csv(input_pet_file)
        self.eia_categories = EiaCategories(df_pet_lower)
        # get categories
        df_categories = self.eia_categories.df_all_categories
        # create a dropdown for each category
        self.dd_children = []
        for col in df_categories.columns.values:
            cat_values = [v for v in df_categories[col].values if len(v)>0]
            options = [{'label':c,'value':c} for c in cat_values]
            value = None
            if col in default_categories_dict.keys():
                value = default_categories_dict[col]
            dd = dcc.Dropdown(id=self._mkid(f'options_{col}'),options=options,value=value,
                              multi=True,
                              placeholder=col,
                              optionHeight=63)
            self.dd_children.append(dd)
        # default dropdowns_div style is row of dropdowns
        default_dropdowns_div_style={
                'display':'grid',
                'grid-template-columns':' '.join(['1fr' for _ in range(len(self.dd_children))]),
                'grid-template-rows':'1fr'
                }
        dropdowns_div = html.Div(
            self.dd_children,id=self._mkid("dropdowns_div"),
            style=default_dropdowns_div_style
            ) 
        self.dropdowns_store = dcc.Store(id=self._mkid('dropdowns_store')) 
        self.children = [dropdowns_div,self.dropdowns_store]  
    
    def register_app(self,theapp):
        @theapp.callback(
            Output(self.dropdowns_store.id,'data'),
            [Input(dd.id,'value') for dd in self.dd_children]
            )
        def _update_dropdowns_store(*arg):
#             print(f'EiaCategoriesDiv._update_dropdowns_store args:{arg}')
            self.logger.info(f'EiaCategoriesDiv._update_dropdowns_store args:{arg}')
            dropdowns_store_data = {}
            id_first_part = self._mkid(f'options_')
            for i in range(len(self.dd_children)):
                this_category = re.split(id_first_part,self.dd_children[i].id)[-1]
                this_value =  arg[i]
                dropdowns_store_data[this_category] = this_value
        
            return dropdowns_store_data
        
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
        
        self.eia_cat_div = EiaCategoriesDiv(self._mkid('eia_cat_div'))
        
        self.data_dt = _make_dt(self._mkid('data_dt'),)
        self.input_pet_file = input_pet_file
        self.children = dcc.Loading(
            children=[self.trigger_interval,
                      self.eia_cat_div,
                      self.num_displayable_columns,
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
                Input(self.num_displayable_columns.id,'value'),
                Input(self.eia_cat_div.dropdowns_store.id,'data')
                ]
            )
        def _populate_data_dt(df_main_store_data,num_displayable_columns,dropdowns_store):
            
            print(f"EiaAccess._populate_data_dt number of columns:{len(df_main_store_data.columns.values)}")
            if dropdowns_store is None:
                columns = df_main_store_data.columns.values[:num_displayable_columns]
            else:
                padd_name = dropdowns_store['padd_name']
                padd_location = dropdowns_store['padd_location']
                padd_production_type = dropdowns_store['padd_production_type']
                padd_fuel_type = dropdowns_store['padd_fuel_type']
                gas_price_region = dropdowns_store['gas_price_region']
                gas_price_gas_type = dropdowns_store['gas_price_gas_type']
                gas_price_grade = dropdowns_store['gas_price_grade']
                
                df_cols = self.eia_cat_div.eia_categories.get_column_set(
                    padd_name,padd_location,padd_production_type,padd_fuel_type,
                    gas_price_region,gas_price_gas_type,gas_price_grade
                    )
                columns = list(set(['date'] + list(df_cols.col.values)))
                # make date the first column
                columns = ['date'] + [c for c in columns if c!='date']
                columns = columns[:num_displayable_columns]
            
            df_ret = df_main_store_data[columns]
            ret_columns = [{'label':c,'id':c} for c in columns]
            
            dict_ret = df_ret.to_dict('records')
            return dict_ret,ret_columns
        self.eia_cat_div.register_app(theapp)
        self.logger.info('registered eia_access')
        
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


    