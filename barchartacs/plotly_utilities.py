# ## Useful utilities for merging data and graphing
import pandas as pd
import numpy as np
import datetime

import matplotlib.pyplot as plt

from matplotlib.collections import PatchCollection
from matplotlib import gridspec
import matplotlib.patches as patches

import plotly.graph_objs as go
from plotly.graph_objs.layout import Font,Margin
from plotly.offline import iplot
import plotly.tools as tls
import pandasql as psql
import importlib
import re
import logging

def iplt(fig):
    iplot(fig)

def str_to_yyyymmdd(d,sep='-'):
    try:
        dt = datetime.datetime.strptime(str(d)[:10],f'%Y{sep}%m{sep}%d')
    except:
        return None
    s = '%04d%02d%02d' %(dt.year,dt.month,dt.day)
    return int(s)

def str_to_date(d,sep='-'):
    try:
        dt = datetime.datetime.strptime(str(d)[:10],f'%Y{sep}%m{sep}%d')
    except:
        return None
    return dt


def add_ymdhm(df_in,date_column):
    df = df_in.copy()
    date_example_type = type(df.iloc[0].date)
    df['dstring'] = df[date_column].apply(lambda d:str(d))
    if len(str(df.iloc[0].dstring))<=8:
        df['year'] = df.dstring.str[0:4].astype(int)
        df['month'] = df.dstring.str[4:6].astype(int)
        df['day'] = df.dstring.str[6:8].astype(int)
    else:
        df['year'] = df.dstring.str[0:4].astype(int)
        df['month'] = df.dstring.str[5:7].astype(int)
        df['day'] = df.dstring.str[8:10].astype(int)
        if len(str(df.iloc[0].dstring))>10:
            df['hour'] = df.dstring.str[11:13].astype(int)
            df['minute'] = df.dstring.str[14:16].astype(int)
    return df

def figure_crosshairs(fig):
    ''' add crosshairs to plotly_plot figure
    '''
    fig['layout'].hovermode='x'
    fig['layout'].yaxis.showspikes=True
    fig['layout'].xaxis.showspikes=True
    fig['layout'].yaxis.spikemode="toaxis+across"
    fig['layout'].xaxis.spikemode="toaxis+across"
    fig['layout'].yaxis.spikedash="solid"
    fig['layout'].xaxis.spikedash="solid"
    fig['layout'].yaxis.spikethickness=1
    fig['layout'].xaxis.spikethickness=1
    fig['layout'].spikedistance=1000
    return fig




def plot_pandas(df_in,x_column,num_of_x_ticks=20,bar_plot=False,figsize=(16,10),use_secondary_yaxis=True):
    '''
    '''
    df_cl = df_in.copy()
    df_cl.index = list(range(len(df_cl)))
    df_cl = df_cl.drop_duplicates()
    xs = list(df_cl[x_column])
    df_cl[x_column] = df_cl[x_column].apply(lambda i:str(i))

    x = list(range(len(df_cl)))
    n = len(x)
    s = num_of_x_ticks
    x_indices = x[::n//s][::-1]
    x_labels = [str(t) for t in list(df_cl.iloc[x_indices][x_column])]
    ycols = list(filter(lambda c: c!=x_column,df_cl.columns.values))
    all_cols = [x_column] + ycols
    if bar_plot:
        if len(ycols)>1:
            if use_secondary_yaxis:
                ax = df_cl[ycols].plot.bar(secondary_y=ycols[1:],figsize=figsize)
            else:
                ax = df_cl[ycols].plot.bar(figsize=figsize)
        else:
            ax = df_cl[ycols].plot.bar(figsize=figsize)
    else:
        if len(ycols)>1:
            if use_secondary_yaxis:
                ax = df_cl[ycols].plot(secondary_y=ycols[1:],figsize=figsize)
            else:
                ax = df_cl[ycols].plot(figsize=figsize)
        else:
            ax = df_cl[ycols].plot(figsize=figsize)

    ax.set_xticks(x_indices)
    ax.set_xticklabels(x_labels, rotation='vertical')
    ax.grid()
    ax.figure.set_size_inches(figsize)
    return ax



def multi_plot(df,x_column,save_file_prefix=None,save_image_folder=None,dates_per_plot=100,num_of_x_ticks=20,figsize=(16,10),bar_plot=False):
    plots = int(len(df)/dates_per_plot) + 1 if len(df) % dates_per_plot > 0 else 0
    f = plt.figure()
    image_names = []
    all_axes = []
    for p in range(plots):
        low_row = p * dates_per_plot
        high_row = low_row + dates_per_plot
        df_sub = df.iloc[low_row:high_row]
        ax = plot_pandas(df_sub,x_column,num_of_x_ticks=num_of_x_ticks,figsize=figsize,bar_plot= bar_plot)
        all_axes.append(ax)
        if save_file_prefix is None or save_image_folder is None:
            continue
        image_name = f'{save_image_folder}/{save_file_prefix}_{p+1}.png'
        ax.get_figure().savefig(image_name)
        image_names.append(image_name)
    return (all_axes,image_names)

def multi_df_plot(dict_df,x_column,save_file_prefix=None,save_image_folder=None,num_of_x_ticks=20,figsize=(16,10),bar_plot=False):
    f = plt.figure()
    image_names = []
    all_axes = []
    p = 0
    for k in dict_df.keys():
        df = dict_df[k]
        ax = plot_pandas(df,x_column,num_of_x_ticks=num_of_x_ticks,figsize=figsize,bar_plot= bar_plot)
        ax.set_title(k)
        all_axes.append(ax)
        if save_file_prefix is None or save_image_folder is None:
            continue
        image_name = f'{save_image_folder}/{save_file_prefix}_{k}{p+1}.png'
        ax.get_figure().savefig(image_name)
        image_names.append(image_name)
        p += 1
    return (all_axes,image_names)





def reload_module(module_name):
    importlib.reload(module_name)


def plotly_pandas(df_in,x_column,num_of_x_ticks=20,plot_title=None,
                  y_left_label=None,y_right_label=None,bar_plot=False,figsize=(16,10),number_of_ticks_display=20,use_secondary_yaxis=True):    
    f = plot_pandas(df_in,x_column=x_column,bar_plot=bar_plot,use_secondary_yaxis=use_secondary_yaxis)#.get_figure()
    # list(filter(lambda s: 'get_y' in s,dir(f)))
    plotly_fig = tls.mpl_to_plotly(f.get_figure())
    d1 = plotly_fig['data'][0]
#     number_of_ticks_display=20
    td = list(df_in[x_column]) 
    spacing = len(td)//number_of_ticks_display
    tdvals = td[::spacing]
    d1.x = td
    d_array = [d1]
    if len(plotly_fig['data'])>1:
        d2 = plotly_fig['data'][1]
        d2.x = td
        d2.xaxis = 'x'
        d_array.append(d2)

    layout = go.Layout(
        title='plotly' if plot_title is None else plot_title,
        xaxis=dict(
            ticktext=tdvals,
            tickvals=tdvals,
            tickangle=90,
            type='category'),
        yaxis=dict(
            title='y main' if y_left_label is None else y_left_label
        ),
    )
    if len(d_array)>1:
        layout = go.Layout(
            title='plotly' if plot_title is None else plot_title,
            xaxis=dict(
                ticktext=tdvals,
                tickvals=tdvals,
                tickangle=90,
                type='category'),
            xaxis2=dict(
                ticktext=tdvals,
                tickvals=tdvals,
                tickangle=90,
                type='category'),
            yaxis=dict(
                title='y main' if y_left_label is None else y_left_label
            ),
            yaxis2=dict(
                title='y alt' if y_right_label is None else y_right_label,
                overlaying='y',
                side='right')
        )
        
    fig = go.Figure(data=d_array,layout=layout)

    if bar_plot:  # fix y values, which have all become positive
        df_yvals = df_in[[c for c in df_in.columns.values if c != x_column]]
        for i in range(len(df_yvals.columns.values)):
            fig.data[i].y = df_yvals[df_yvals.columns.values[i]]
        
    return fig


def plotly_plot(df_in,x_column,plot_title=None,
                y_left_label=None,y_right_label=None,
                bar_plot=False,width=800,height=400,
                number_of_ticks_display=20,
                yaxis2_cols=None,
                x_value_labels=None,
                modebar_orientation='v',modebar_color='grey',
                legend_x=None,legend_y=None,
                title_y_pos = 0.9,
                title_x_pos = 0.5,
                add_crosshairs=True,
                go_functions = None,
                opacity_list = None,
                center_title = True
               ):
    
    ya2c = [] if yaxis2_cols is None else yaxis2_cols
    ycols = [c for c in df_in.columns.values if c != x_column]
    # create tdvals, which will have x axis labels
    td = list(df_in[x_column]) if x_column is not None else df_in.index.values
    nt = len(df_in)-1 if number_of_ticks_display > len(df_in) else number_of_ticks_display
    spacing = len(td)//nt
    tdvals = td[::spacing]
    tdtext = tdvals
    if x_value_labels is not None:
        tdtext = [x_value_labels[i] for i in tdvals]
    
    # create data for graph
    data = []
    # iterate through all ycols to append to data that gets passed to go.Figure

    # determine if each ycol is of type go.Bar, go.Scatter, etc
    # pfs = "plot functions"
    pfs = go_functions
    if pfs is None:
        if bar_plot:
            pfs = [go.Bar for _ in range(len(ycols))]
        else:
            pfs = [go.Scatter for _ in range(len(ycols))]

    # determine each ycol's opacity
    ops = opacity_list
    if ops is None:
        ops = [1 for _ in range(len(ycols))]
        
    for i in range(len(ycols)):
        ycol = ycols[i]
        b = pfs[i](x=td,y=df_in[ycol],name=ycol,opacity=ops[i],yaxis='y' if ycol not in ya2c else 'y2')
        data.append(b)

    # create a layout

    layout = go.Layout(
        title=plot_title,
        xaxis=dict(
            ticktext=tdtext,
            tickvals=tdvals,
            tickangle=45,
            type='category'),
        yaxis=dict(
            title='y main' if y_left_label is None else y_left_label
        ),
        yaxis2=dict(
            title='y alt' if y_right_label is None else y_right_label,
            overlaying='y',
            side='right'),
        autosize=True,
#         autosize=False,
#         width=width,
#         height=height,
        margin=Margin(
            b=100
        ),
        modebar={'orientation': modebar_orientation,'bgcolor':modebar_color}
    )

    fig = go.Figure(data=data,layout=layout)
    fig.update_layout(
        title={
            'text': plot_title,
            'y':title_y_pos,
            'x':title_x_pos,
            'xanchor': 'center',
            'yanchor': 'top'})
    if (legend_x is not None) and (legend_y is not None):
        fig.update_layout(legend=dict(x=legend_x, y=legend_y))
    if add_crosshairs:
        fig = figure_crosshairs(fig)
    if center_title:
        fig.update_layout(title_x=0.5)
    return fig

def plotly_bar_plot(df_in,x_column,num_of_x_ticks=20,plot_title=None,
                  y_left_label=None,y_right_label=None,bar_plot=False,figsize=(16,10),
                    number_of_ticks_display=20,use_secondary_yaxis=True):
    ycols = [c for c in df_in.columns.values if c != x_column]
    # create tdvals, which will have x axis labels
    td = list(df_in[x_column]) 
    spacing = len(td)//number_of_ticks_display
    tdvals = td[::spacing]
    
    # create data for graph
    data = []
    # iterate through all ycols to append to data that gets passed to go.Figure
    for ycol in ycols:
        b = go.Bar(x=td,y=df_in[ycol],name=ycol)
        data.append(b)

    # create a layout
    layout = go.Layout(
        title=plot_title,
        xaxis=dict(
            ticktext=tdvals,
            tickvals=tdvals,
            tickangle=90,
            type='category'),
        yaxis=dict(
            title='y main' if y_left_label is None else y_left_label
        ),
        yaxis2=dict(
            title='y alt' if y_right_label is None else y_right_label,
            overlaying='y',
            side='right')
    )

    fig = go.Figure(data=data,layout=layout)
    return fig

def candles(df,title=None,open_column='open',high_column='high',low_column='low',close_column='close',volume_column='volume',
            date_column='timestamp',num_of_x_ticks=20,bar_width=.5,
           min_volume=None,max_volume=None,figsize=None,date_offset_to_show=(11,16)):
    def __full_date(d):
        sd = ''.join(re.findall('[0-9]{1,4}',str(d)))
#         year = int(str(d)[0:4])
#         month = int(str(d)[5:7])
#         day = int(str(d)[8:10])
#         hour = int(str(d)[11:13])
#         minute = int(str(d)[14:16])
        l = len(sd)
        year = int(sd[0:4])
        month = int(sd[4:6])
        day = int(sd[6:8])
        hour = int(sd[8:10]) if l>=10 else 0
        minute = int(sd[10:12]) if l>=12 else 0
        dt = datetime.datetime(year,month,day,hour,minute)
        return(dt)
    
    # Step 1: create fig and axises
    # force the "non-body" lines of the candlestick to be drawn from the middle of the candlestick body
    line_offset = bar_width/2
    
    fs = (15,8) if figsize is None else figsize
    # create a "gridspec" dictionary that determines the relative size of the candlestick chart vs the volume chart
    gs_kw = dict(width_ratios=[1], height_ratios=[1,.3])
    # create the figure and the 2 axis so that you can "subplot" the candlesticks and the volume bars separately.

    
    fig,axs = plt.subplots(2,1,figsize=fs, gridspec_kw=gs_kw, sharex=True)
    fig.subplots_adjust(hspace=0)
    # Step 2: build dataframe to plot
    df_2 = df.copy()
    # add a very small amout th the close, so that the close - open will not be zero
    df_2[close_column] = df_2[close_column]+.00001
    # create a date column of datetime objects
    df_2['date'] = df_2[date_column].apply(__full_date)
    # make the index a range of integers
    df_2.index = list(range(len(df_2)))
    # get the absolute high's and low's for both the x and y axis of the candlestick graph for this day
    ymin = df_2[low_column].min()
    ymax = df_2[high_column].max() #ymin + largest_height
    xmin = df_2.index.min()
    xmax = df_2.index.max()
    # set the x and y limits to the candlestick graph
    axs[0].set_xlim(xmin,xmax)
    axs[0].set_ylim(ymin,ymax)
    axs[0].set_title(f'{"Candle Chart" if title is None else title}')    

    # Step 3: create rectangles to display all "green body UP day" candlesticks, where the close is >= the open
    df_3 = df_2[df_2[close_column]>=df_2.open][[open_column,close_column,high_column,low_column]]
    # You create a candlestick by creating a Rectangle object.
    x_left_edge = np.array(df_3.index)
    y_left_edge = np.array(df_3[open_column])
    y_bottom_line_min = np.array(df_3[low_column])
    y_bottom_line_max = y_left_edge
    y_top_line_min = np.array(df_3[close_column])
    y_top_line_max = np.array(df_3[high_column])
    heights = np.array(df_3[close_column] - df_3[open_column])

    # Step 3.2 create a rectangle for the candlestick body of each "up" day 
    for i in range(len(df_3)):
        xle = x_left_edge[i]
        yle = y_left_edge[i]
        h = heights[i]
        r = patches.Rectangle([xle,yle],bar_width,h,linewidth=1,color='g')
        r.set_fill(True)
        axs[0].add_patch(r)
    # Now create the verticle lines that run from the top and bottom of the candlestick body
    axs[0].vlines(x_left_edge+line_offset, y_bottom_line_min, y_bottom_line_max,colors=['m' for _ in range(len(x_left_edge))])
    axs[0].vlines(x_left_edge+line_offset, y_top_line_min, y_top_line_max,colors=['m' for _ in range(len(x_left_edge))])

    # Step 4: create rectangles to display all "red body DOWN day" candlesticks, where the close is < the open
    df_3 = df_2[df_2[close_column]<df_2.open][[open_column,close_column,high_column,low_column]]
    x_left_edge = np.array(df_3.index)
    y_left_edge = np.array(df_3[close_column])
    y_bottom_line_min = np.array(df_3[low_column])
    y_bottom_line_max = y_left_edge
    y_top_line_min = np.array(df_3[open_column])
    y_top_line_max = np.array(df_3[high_column])
    heights = np.array(df_3.open-df_3[close_column])
    for i in range(len(df_3)):
        xle = x_left_edge[i]
        yle = y_left_edge[i]
        h = heights[i]
        r = patches.Rectangle([xle,yle],bar_width,h,linewidth=1,color='r')
        axs[0].add_patch(r)
    # Now create the verticle lines that run from the top and bottom of the candlestick body
    axs[0].vlines(x_left_edge+line_offset, y_bottom_line_min, y_bottom_line_max,colors=['m' for _ in range(len(x_left_edge))])
    axs[0].vlines(x_left_edge+line_offset, y_top_line_min, y_top_line_max,colors=['m' for _ in range(len(x_left_edge))])

        
    # Step 3: Create the volume plot using rectangle objects
    x_left_edge = np.array(df_2.index)
    heights = np.array(df_2[volume_column])
    for i in range(len(df_2)):
        xle = x_left_edge[i]
        yle = 0
        h = heights[i]
        r = patches.Rectangle([xle,0],bar_width,h,linewidth=1,color='b')
        axs[1].add_patch(r)
    
    # set y axis scale for the volume graph
    minv = df_2[volume_column].min() if min_volume is None else min_volume
    maxv = df_2[volume_column].max() if max_volume is None else max_volume
    axs[1].set_ylim(minv,maxv)
    
    # Step 4: Create the x-axis values that will be displayed on the graph
    x = list(range(len(df_2)))
    n = len(x)
    s = num_of_x_ticks
    x_indices = x[::n//s][::-1]
    x_labels = [str(t)[date_offset_to_show[0]:date_offset_to_show[1]] for t in list(df_2.iloc[x_indices][date_column])]
    axs[0].set_xticks(x_indices)
    axs[0].set_xticklabels(x_labels, rotation=90)
    axs[0].grid()
    axs[1].set_xticks(x_indices)
    axs[1].set_xticklabels(x_labels, rotation=90)
    axs[1].grid() 
    return fig, axs