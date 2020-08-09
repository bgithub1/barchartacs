'''
Created on Aug 9, 2020

@author: bperlman1
'''
from dashapp_db_table_access import dcc,html,Output,Input
from dashapp_csvaccess_app import app,app_port,url_base_pathname
from dashapp_csvaccess_zipaccess_app import app_layout as ziplayout
from dashapp_csvaccess_sqlaccess_app import app_layout as sqllayout

# Create app.

loc_comp = dcc.Location(id='csvaccess_url', refresh=False)
page_comp = html.Div(['initial content'],id='page-content')
ziplink = dcc.Link('zip', href=f'{url_base_pathname}/zip')
sqllink = dcc.Link('sql', href=f'{url_base_pathname}/sql')
app.layout = html.Div([
    loc_comp,ziplink,html.Br(),sqllink,page_comp
])


@app.callback(Output(page_comp.id, 'children'),
              [Input(loc_comp.id, 'pathname')])
def display_page(pathname):
    print(f"display_page pathname: {pathname}")
    if pathname == f'{url_base_pathname}/zip':
        
        return ziplayout
    elif pathname == f'{url_base_pathname}/sql':
        return sqllayout
    else:
        return '404'

if __name__ == '__main__':
    app.run_server(port=app_port,debug=False)
