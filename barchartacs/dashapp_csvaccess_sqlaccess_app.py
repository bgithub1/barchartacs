'''
Created on Aug 2, 2020

Use dash_extensions to query and display large DataFrames that come from zip file queries

@author: bperlman1
'''
from dashapp_csvaccess_app import app
from dashapp_db_table_access import Input,html,SqlAccess,CsvViewer


sql_access = SqlAccess(app, 'sql_viewer')
csv_viewer = CsvViewer(app, 'sql_viewer',sql_access.main_store,
                       num_rows_input=Input(sql_access.num_rows_input.id,'value'),
                       button_style = {"background-color": "#e7e7e7", "color": "black"}
                       )
app_layout = html.Div([sql_access,csv_viewer])
sql_access.register_app(app)
csv_viewer.register_app(app)
print('registered sql_access')
    