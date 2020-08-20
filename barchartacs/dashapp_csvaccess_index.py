'''
Created on Aug 9, 2020

@author: bperlman1
'''
import sys,os
sys.path.append(os.path.abspath('./'))
sys.path.append(os.path.abspath('../'))

from dashapp_db_table_access import dcc,html,Output,Input#@UnResolvedImport
from dashapp_csvaccess_app import app,app_port,url_base_pathname#@UnResolvedImport
from dashapp_csvaccess_zipaccess_app import app_layout as ziplayout#@UnResolvedImport

# Create app.
padd = 1

app.layout = html.Div([ziplayout],
                      style={'padding-right': f'{padd}%','padding-left': f'{padd}%'}
    )


if __name__ == '__main__':
    app.run_server(port=app_port,debug=False)
