'''
Created on Jul 24, 2020

@author: bperlman1
'''
from dashapp import single_page_from_df as spfd#@UnresolvedImport
import argparse as ap

if __name__=='__main__':
    parser =  ap.ArgumentParser()
    parser.add_argument('--host',type=str,
                        help='host (default 127.0.0.1)',
                        default='127.0.0.1')
    parser.add_argument('--port',type=int,
                        help='port (default 8811)',
                        default=8811)
    parser.add_argument('--basepath',type=str,
                        help='url_base_pathname (default app8811)',
                        default='/app8811/')
    
    args = parser.parse_args()
    host = args.host
    port = args.port
    basepath = args.basepath
    if basepath[0] != '/':
        basepath[0] = '/'
    if basepath[-1] != '/':
        basepath[-1] = '/'
    logger = spfd.dashapp.init_root_logger()    
    spfd.graph_from_csv_page(
        main_id='myid',app_port = port,
        app_host = host,
        app_title="csv_grapher",
        url_base_pathname=basepath,
        logger=logger)