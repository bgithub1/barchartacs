'''
Created on Aug 2, 2020

Use dash_extensions to query and display large DataFrames that come from zip file queries

@author: bperlman1
'''
from dashapp_csvaccess_app import app#@UnResolvedImport
from dashapp_db_table_access import Input,html,ZipAccess,FeatureSelector#@UnResolvedImport

ftype='zip'
zip_access = ZipAccess(app, f'{ftype}_feature_viewer',ftype)
feature_selector = FeatureSelector(
                    app, f'{ftype}_feature_viewer',zip_access.main_store,
                    num_rows_input=Input(zip_access.num_rows_input.id,'value'),
                    button_style = {"background-color": "#e7e7e7", "color": "black"}
                    )

app_layout = html.Div([zip_access,feature_selector])
zip_access.register_app(app)
feature_selector.register_app(app)
print(f'registered {ftype}_feature_selector')

    