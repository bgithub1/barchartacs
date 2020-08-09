'''
Created on Aug 9, 2020

@author: bperlman1
'''
from dash_extensions.enrich import Dash


app_port = 8812

url_base_pathname=f'/app{app_port}/'
app = Dash(prevent_initial_callbacks=True,url_base_pathname=url_base_pathname)
app.title='db_table_access'
server = app.server
