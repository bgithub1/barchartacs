'''
Created on Aug 2, 2020

Use dash_extensions to query and display large DataFrames that come from zip file queries

@author: bperlman1
'''
# from dashapp_csvaccess_app import app#@UnResolvedImport
from dashapp_db_table_access import Dash,Input,dcc,ZipAccess,CsvViewer,FeatureSelector#@UnResolvedImport
# from barchartacs.dashapp_csvaccess_feature_app import feature_selector

app_port = 8812

url_base_pathname=f'/app{app_port}/'
app = Dash(prevent_initial_callbacks=True,url_base_pathname=url_base_pathname)
app.title='db_table_access'
# server = app.server

zip_access = ZipAccess(app, 'zip_viewer')

feature_selector = FeatureSelector(
                    app, 'zip_feature_viewer',zip_access.main_store,
                    num_rows_input=Input(zip_access.num_rows_input.id,'value'),
                    button_style = {"background-color": "#e7e7e7", "color": "black"}
                    )

csv_viewer = CsvViewer(app, 'zip_csv_viewer',zip_access.main_store,
                       initial_columns_store=feature_selector.dt_data,
                       initial_columns_store_colname=feature_selector.column_info_colname,
                       num_rows_input=Input(zip_access.num_rows_input.id,'value'),
                       button_style = {"background-color": "#e7e7e7", "color": "black"}
                       )
t1 = dcc.Tab(children=feature_selector,
                label='Feature selector',value='feature_selector')
t2 = dcc.Tab(
    children=csv_viewer,
        label='Csv Viewer',
        value='csv_viewer'
        )
        
tabs = dcc.Tabs(
    children=[t1,t2], value='feature_selector',
    style={
    'display':'grid',
    'grid-template-columns':'1fr 1fr',
    'grid-template-rows':'1fr'
    }
)

app_layout = dcc.Loading(children=[zip_access,tabs],
                         fullscreen=True, type="dot")

app.layout = app_layout
zip_access.register_app(app)
feature_selector.register_app(app)
csv_viewer.register_app(app)
print('registered zip_access')
if __name__ == '__main__':
    app.run_server(port=app_port,debug=False)

    