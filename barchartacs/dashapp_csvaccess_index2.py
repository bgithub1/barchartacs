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
from dashapp_csvaccess_csvaccess_app import app_layout as csvlayout#@UnResolvedImport
from dashapp_csvaccess_sqlaccess_app import app_layout as sqllayout#@UnResolvedImport
from dashapp_csvaccess_feature_app import app_layout as featurelayout#@UnResolvedImport

# Create app.

loc_comp = dcc.Location(id='csvaccess_url', refresh=False)
page_comp = html.Div(['initial content'],id='page-content')
ziplink = dcc.Link('zip', href=f'{url_base_pathname}/zip')
csvlink = dcc.Link('csv', href=f'{url_base_pathname}/csv')
sqllink = dcc.Link('sql', href=f'{url_base_pathname}/sql')
featurelink = dcc.Link('feature', href=f'{url_base_pathname}/feature')
padd = 1
all_links = [ziplink,csvlink,sqllink,featurelink]
gtc_links = ' '.join(['1fr' for _ in range(len(all_links))])
link_choices_div = html.Div(
    all_links,
    style={
           'display':'grid','grid-template-columns':gtc_links,
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
    elif pathname == f'{url_base_pathname}/csv':
        return csvlayout
    elif pathname== f'{url_base_pathname}/feature':
        return featurelayout
    else:
        return html.Div([html.Br(),'****** Choose a Data Source from the Links Above **********'])

if __name__ == '__main__':
    app.run_server(port=app_port,debug=False)
