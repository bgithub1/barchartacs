'''
Created on Aug 2, 2020

Use dash_extensions to query and display large DataFrames that come from zip file queries

@author: bperlman1
'''
from dashapp_csvaccess_app import app#@UnResolvedImport
from dashapp_db_table_access import Input,html,SqlAccess,CsvViewer#@UnResolvedImport
import sys
argvs = sys.argv
app_port = 8812
if len(argvs)>1:
    app_port = int(argvs[1])
sql_access = SqlAccess(app, 'sql_viewer')
csv_viewer = CsvViewer(app, 'sql_viewer',sql_access.main_store,
                       num_rows_input=Input(sql_access.num_rows_input.id,'value'),
                       button_style = {"background-color": "#e7e7e7", "color": "black"}
                       )
app_layout = html.Div([sql_access,csv_viewer])
app.layout = app_layout
app.title='sql_table_access'
sql_access.register_app(app)
csv_viewer.register_app(app)

print('registered sql_access')
if __name__ == '__main__':
    app.run_server(port=app_port,debug=False)
    