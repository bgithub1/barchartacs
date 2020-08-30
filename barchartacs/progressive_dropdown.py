'''
Created on Aug 13, 2020

@author: bperlman1
'''

import dash_core_components as dcc
import dash_html_components as html
from dash_extensions.enrich import Output, Input
from dash.exceptions import PreventUpdate
import pandas as pd

def make_text_centered_div(text):    
    col_inner_style = {
        'margin':'auto',
        'word-break':'break-all',
        'word-wrap': 'break-word'
    }
    return html.Div([text],style=col_inner_style)

def multi_row_panel(rows):
    s = {'display':'grid',
         'grid-template-rows':' '.join(['1fr' for _ in range(len(rows))]),
         'grid-template-columns':'1fr'
        }
    return html.Div(rows,style=s)

class ProgressiveDropdown(html.Div):
    def __init__(self,
                 init_values_source,
                 dropdown_id,
                 number_of_dropdowns,                          
                 label_list=None,
                 title_list=None,
                 use_title_divs=True
                 ):
        self.init_values_source = init_values_source
        current_parent = None
        pd_dd_list = []
        pd_div_list = []
    #     current_value_list = value_list.copy()
        for i in range(number_of_dropdowns):
            curr_id = f"{dropdown_id}_v{i}"
            title = None if (title_list is None) or (len(title_list) < i+1) else title_list[i]
            pd_dd = _ProgressiveDropdownChild(
                curr_id,current_parent,placeholder=title)
            current_parent = pd_dd
            # wrap dropdown with title 
            dropdown_rows = [pd_dd]
            if (title_list is not None) and (len(title_list)>i) and use_title_divs:
                title_div = make_text_centered_div(title_list[i])
                dropdown_rows = [title_div,pd_dd]
            dropdown_div = multi_row_panel(dropdown_rows)
            # append new dropdown, wrapped in title, to list of dropdown htmls
            pd_dd_list.append(pd_dd)
            pd_div_list.append(dropdown_div)
        
        self.pd_dd_list = pd_dd_list
        self.pd_div_list = pd_div_list
        super(ProgressiveDropdown,self).__init__(
            self.pd_div_list,id=f"{dropdown_id}_progressive_dropdown_div"
            )
        
    def register_app(self,theapp):
        @theapp.callback(
            Output(self.pd_dd_list[0].id,'options'),
            [Input(self.init_values_source.id,'data')]
            )
        def _update_first_dropdown(dict_df):
            if dict_df is None or len(dict_df)<=0:
                raise PreventUpdate("progressive_dropdowns._init_all_dropdowns no data")
            df = pd.DataFrame(dict_df).iloc[:1]
            cols = df.columns.values
            initial_parent_options = [{'label':c,'value':c} for c in cols]
            print('initial_parent_options')
            print(initial_parent_options)
            return initial_parent_options
        for prog_dd in self.pd_dd_list:
            prog_dd.register_app(theapp)


class _ProgressiveDropdownChild(dcc.Dropdown):
    def __init__(self,
                 dropdown_id,parent_dropdown,multi=True,
                 placeholder=None,
                 optionHeight=70
                 ):
        self.parent_dropdown = parent_dropdown
        super(_ProgressiveDropdownChild,self).__init__(
            id=dropdown_id,
            multi=multi,
            placeholder=placeholder,
            optionHeight=optionHeight
        )
        
    def register_app(self,theapp):
        if self.parent_dropdown is None:
            return
        @theapp.callback(
            Output(self.id,'options'),
            [
                Input(self.parent_dropdown.id,'value'),
                Input(self.parent_dropdown.id,'options')]
            )
        def _choose_options(parent_value,parent_options):
            if type(parent_value) != list:
                parent_value = [parent_value]            
            child_options = [po for po in parent_options if po['value'] not in parent_value]
            return child_options
