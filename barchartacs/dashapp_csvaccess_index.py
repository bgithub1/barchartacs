'''
Created on Aug 9, 2020

@author: bperlman1
'''
from dashapp_db_table_access import dcc,html,Output,Input#@UnResolvedImport
from dashapp_csvaccess_app import app,app_port,url_base_pathname#@UnResolvedImport
from dashapp_csvaccess_zipaccess_app import app_layout as ziplayout#@UnResolvedImport
from dashapp_csvaccess_sqlaccess_app import app_layout as sqllayout#@UnResolvedImport

# Create app.

loc_comp = dcc.Location(id='csvaccess_url', refresh=False)
page_comp = html.Div(['initial content'],id='page-content')
ziplink = dcc.Link('zip', href=f'{url_base_pathname}/zip')
sqllink = dcc.Link('sql', href=f'{url_base_pathname}/sql')
padd = 1
link_choices_div = html.Div(
    [ziplink,sqllink],
    style={
           'display':'grid','grid-template-columns':'1fr 1fr ',
           'grid-template-rows':'1fr'
           })
app.layout = html.Div(
    [loc_comp,link_choices_div,page_comp],
    style={'padding-right': f'{padd}%','padding-left': f'{padd}%'}
    )


@app.callback(Output(page_comp.id, 'children'),
              [Input(loc_comp.id, 'pathname')])
def display_page(pathname):
    print(f"display_page pathname: {pathname}")
    if pathname == f'{url_base_pathname}/zip':
        
        return ziplayout
    elif pathname == f'{url_base_pathname}/sql':
        return sqllayout
    else:
        return html.Div([html.Br(),'****** Choose a Data Source from the Links Above **********'])

if __name__ == '__main__':
    app.run_server(port=app_port,debug=False)
