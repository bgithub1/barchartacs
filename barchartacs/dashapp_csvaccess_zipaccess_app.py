'''
Created on Aug 2, 2020

Use dash_extensions to query and display large DataFrames that come from zip file queries

@author: bperlman1
'''
from dashapp_csvaccess_app import app#@UnResolvedImport
from dashapp_db_table_access import Input,html,ZipAccess,CsvViewer#@UnResolvedImport


zip_access = ZipAccess(app, 'zip_viewer')
csv_viewer = CsvViewer(app, 'zip_viewer',zip_access.main_store,
                       num_rows_input=Input(zip_access.num_rows_input.id,'value'),
                       button_style = {"background-color": "#e7e7e7", "color": "black"}
                       )
app_layout = html.Div([zip_access,csv_viewer])
zip_access.register_app(app)
csv_viewer.register_app(app)
print('registered zip_access')

    