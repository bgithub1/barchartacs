#!/usr/local/bin/python2.7
# encoding: utf-8
'''

build_db.py helps create the DataFrames that you use to 
    populate the sec_schema.options_table postgres table.
    You can use the main in this module to re-create, and re-populate 
    that table from the BuildDb class, or you can only build 
    the DataFrame that you then sql "COPY" to postgres, 
    but use the options_table_loader.ipynb jupyter notebook instead.
    
    
@author:     bill perlman

'''

import zipfile
import glob
import pandas as pd
import numpy as np

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
import sys
import os

if  not './' in sys.path:
    sys.path.append('./')
if  not '../' in sys.path:
    sys.path.append('../')
from barchartacs import pg_pandas as pg
import datetime as dt
import io
from tqdm import tqdm
import re
import logging




__all__ = []
__version__ = 0.1
__date__ = '2019-06-27'
__updated__ = '2019-06-27'

DEBUG = 1
TESTRUN = 0
PROFILE = 0


def init_root_logger(logfile,logging_level=None):
    level = logging_level
    if level is None:
        level = logging.DEBUG
    # get root level logger
    logger = logging.getLogger()
    if len(logger.handlers)>0:
        return logger
    logger.setLevel(logging.getLevelName(level))

    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)   
    return logger

class BuildDb():
    BUILDDB_DB_NAME = 'sec_db'
    BUILDDB_SCHEMA_NAME = 'sec_schema'

    def __init__(self,zip_folder_parent,
                 yyyymm=None,
                 strike_divisor = None,
                 strike_divisor_dict=None,
                 unzip_folder=None,
                 zipname_first_part=None,
                 options_table_name=None,
                 underlying_table_name=None,
                 dburl=None,
                 databasename=None,
                 username=None,
                 password=None,
                 schema_name=None,
                 recreate_schema=False,
                 recreate_tables=False,
                 contract_list = None,
                 write_to_database=False,
                 logger = None
                 ):
        
        self.yyyymm = yyyymm
        self.strike_divisor_dict = {'GE':100,'NG':100} if strike_divisor_dict is None else strike_divisor_dict
        self.strike_divisor = 1 if strike_divisor is None else strike_divisor
        self.recreate_schema = recreate_schema
        self.recreate_tables = recreate_tables
        
        self.zip_folder_parent = zip_folder_parent
        self.unzip_folder = './temp_folder/unzipfolder' if unzip_folder is None else unzip_folder
        if not os.path.isdir(self.unzip_folder):
            os.mkdir(self.unzip_folder)
                             
        self.zipname_first_part = 'opv' if zipname_first_part is None else zipname_first_part
        
        self.options_table_name = 'options_table' if options_table_name is None else options_table_name
        self.underlying_table_name = 'underlying_table' if underlying_table_name is None else underlying_table_name
        
        self.dburl = dburl if dburl is not None else 'localhost'
        self.username = username if username is not None else ''
        self.password = password if password is not None else ''
        self.databasename = databasename if databasename is not None else BuildDb.BUILDDB_DB_NAME
        self.schema_name = schema_name if schema_name is not None else BuildDb.BUILDDB_SCHEMA_NAME
        
        self.dict_month_names = {
            1:'jan',
            2:'feb',
            3:'mar',
            4:'apr',
            5:'may',
            6:'jun',
            7:'jul',
            8:'aug',
            9:'sep',
            10:'oct',
            11:'nov',
            12:'dec'
            }
        self.options_header = 'contract,month_year,strike_right,date,open,high,low,close,volume,open_interest'
        self.logger = init_root_logger('logfile.log', 'INFO') if logger is None else logger
        self.pga = pg.PgPandas(databasename=self.databasename,username=self.username,password=self.password,dburl=self.dburl)
        try:
            self.pga.get_sql('select * from information_schema.tables')
        except Exception as e:
            self.logger.warn(str(e))
            raise ValueError(f'CANNOT ACCESS postgres database {self.databasename}.  You might have to create it with createdb {self.databasename}')
        self.contract_list = ['CL','CB','CT','CD','D6',
                             'E6','ES','GC','GE','HO',
                             'J6','KC','LB','LC','LH',
                             'ND','NG','NQ','OJ','PA',
                             'PL','RB','SB','SF','ZB',
                             'ZL','ZM','ZN','ZS'
                            ] if contract_list is None else contract_list
        self.write_to_database = write_to_database
        self.regex_options_csv_pattern = 'opv[01][0-9][0-3][0-9][0-9].[cC][sS][vV]'
        self.glob_options_csv_pattern = f'{self.unzip_folder}/opv*.[cC][sS][vV]'
        
    
    def get_strike_divisor(self,contract):
        if self.strike_divisor_dict is None:
            return self.strike_divisor
        commod = contract[:2]
        if commod not in self.strike_divisor_dict.keys():
            return self.strike_divisor
        return self.strike_divisor_dict[commod]
        
    def get_csv_files_from_yyyymm(self,yyyymm):
        mm = str(yyyymm)[4:6]
        csv_name = f'{self.zipname_first_part}{mm}*.csv'
        return csv_name        
        
    
    
    def get_zipfile_from_yyyymm(self):
        yy = str(self.yyyymm)[2:4]
        mm = int(str(self.yyyymm)[4:6])
        sub_folder = f'{self.zipname_first_part}{self.dict_month_names[mm]}{yy}'
        zip_file = f'{self.zip_folder_parent}/{sub_folder}/{sub_folder}.zip'
        return zip_file
        
    def unzip_file(self):
        if self.yyyymm is None:
            raise ValueError('unzip_file ERROR: yyyymm is None. Cannot find a zip file without a valid yyyymm')
        # remove old unzip_folder contents
#         old_csv_paths = glob.glob(f'{self.unzip_folder}/opv[01][0-9][0-3][0-9][0-9].[cC][sS][vV]')
        r = self.regex_options_csv_pattern
        f = self.glob_options_csv_pattern
        old_csv_paths = [p for p in glob.glob(f) if len(re.findall(r,p))>0]
        
        for old_csv in old_csv_paths:
            self.logger.info(f'unzip_file: removing old zip file {old_csv}')
            os.remove(old_csv)
        path_to_zip_file = f'{self.get_zipfile_from_yyyymm()}'
        zip_ref = zipfile.ZipFile(path_to_zip_file, 'r')
        zip_ref.extractall(self.unzip_folder)
        zip_ref.close()
#         csv_paths = glob.glob(f'{self.unzip_folder}/*.[cC][sS][vV]')
        csv_paths = [p for p in glob.glob(f) if len(re.findall(r,p))>0]
        for csv_path in csv_paths:
            os.rename(csv_path,csv_path.lower())
        return [s.lower() for s in csv_paths]
    
    def recreate_pg_tables(self):
        self.make_options_table()
        self.make_underlying_table()
        
    def make_options_table(self):
        try:
            # always try to build the table in case it's the first time
            sql = f"""
            create table {self.schema_name}.{self.options_table_name}(
                symbol text not null,
                strike numeric not null,
                pc text not null,
                settle_date integer not null,
                open numeric not null,
                high numeric not null,
                low numeric not null,
                close numeric not null,
                adj_close numeric not null,
                volume integer not null,
                open_interest integer not null,
                primary key(symbol,settle_date,strike,pc));
            """            
            self.pga.exec_sql_raw(sql) 
        except Exception as e:
            self.logger.warn(f'ERROR make_options_table: {str(e)}')
            # ignore
            pass
        
    def make_underlying_table(self):
        try:
            # always try to build the table in case it's the first time
            sql = f"""
            create table {self.schema_name}.{self.underlying_table_name}(
                symbol text not null,
                settle_date integer not null,
                open numeric not null,
                high numeric not null,
                low numeric not null,
                close numeric not null,
                adj_close numeric not null,
                volume integer not null,
                open_interest integer not null,
                primary key(symbol,settle_date));
            """            
            self.pga.exec_sql_raw(sql) 
        except Exception as e:
            self.logger.warn(f'ERROR make_underlying_table: {str(e)}')
            # ignore
            pass


    def _df_from_text(self,text_path):
        lines = open(text_path,'r').read()
        options_header = 'contract,month_year,strike_right,date,open,high,low,close,volume,open_interest'
        lines2 = options_header + '\n'+ lines
        lines3 = lines2.split('\n')
        s2 = io.StringIO()
        for l in lines3:
            s2.write(l+'\n')
        s2.seek(0)
        df = pd.read_csv(s2)
        return df
    
    def _make_strikes(self,row):
        divisor = self.get_strike_divisor(row.contract)
        strike = float(row.strike_right[0:-1])
        return strike/divisor

    def build_options_pg_from_csvs(self,options_csv_path):
        try:
            p = options_csv_path
            df_temp = self._df_from_text(p)
            df_temp = df_temp[df_temp.contract.isin(self.contract_list)]
            symbols = df_temp.contract + df_temp.month_year.str.slice(0,1)  + df_temp.month_year.str.slice(3,5)
#             strikes = df_temp.strike_right.str.slice(0,-1).astype(float)
#             strikes = strikes / self.strike_divisor            
            strikes = df_temp.apply(self._make_strikes,axis=1)
            pcs = df_temp.strike_right.str.slice(-1,)
            settle_dates = (df_temp.date.astype(str).str.slice(6,10) + df_temp.date.astype(str).str.slice(0,2) + df_temp.date.astype(str).str.slice(3,5)).astype(int)
            opens = df_temp.open.astype(float)
            highs = df_temp.high.astype(float)
            lows = df_temp.low.astype(float)
            closes = df_temp.close.astype(float)
            volumes = df_temp.volume.astype(int)
            open_interests = df_temp.open_interest.astype(int)
            
            df = pd.DataFrame({'symbol':symbols,
                'strike':strikes,
                'pc':pcs,
                'settle_date':settle_dates,
                'open':opens,
                'high':highs,
                'low':lows,
                'close':closes,
                'adj_close':closes,
                'volume':volumes,
                'open_interest':open_interests})
            return df
        except Exception as e:
            self.logger.warn(f'ERROR build_options_pg_from_csvs yyyymm={self.yyyymm} file={p} exception={str(e)}')
        
    
    def build_options_loop(self):
        file_paths = glob.glob(self.glob_options_csv_pattern)
        df_all = None
        for p in tqdm(file_paths):
            try:
                df_temp = self._df_from_text(p)
                df_temp = df_temp[df_temp.contract.isin(self.contract_list)]
                symbols = df_temp.contract + df_temp.month_year.str.slice(0,1)  + df_temp.month_year.str.slice(3,5)
#                 strikes = df_temp.strike_right.str.slice(0,-1).astype(float)
#                 strikes = strikes / self.strike_divisor            
                strikes = df_temp.apply(self._make_strikes,axis=1)
                pcs = df_temp.strike_right.str.slice(-1,)
                settle_dates = (df_temp.date.astype(str).str.slice(6,10) + df_temp.date.astype(str).str.slice(0,2) + df_temp.date.astype(str).str.slice(3,5)).astype(int)
                opens = df_temp.open.astype(float)
                highs = df_temp.high.astype(float)
                lows = df_temp.low.astype(float)
                closes = df_temp.close.astype(float)
                volumes = df_temp.volume.astype(int)
                open_interests = df_temp.open_interest.astype(int)
                
                df = pd.DataFrame({'symbol':symbols,
                    'strike':strikes,
                    'pc':pcs,
                    'settle_date':settle_dates,
                    'open':opens,
                    'high':highs,
                    'low':lows,
                    'close':closes,
                    'adj_close':closes,
                    'volume':volumes,
                    'open_interest':open_interests})
                                
                if self.write_to_database:
                    self.logger.info(f'writing {p} to postgres')
                    self.write_options_df_to_pg(df)
                else:
                    self.logger.info(f'NOT writing {p} to postgres')
                if df_all is None:
                    df_all = df.copy()
                else:
                    df_all = df_all.append(df,ignore_index=True)
                    df_all.index = list(range(len(df_all)))
            except Exception as e:
                self.logger.warn(f'ERROR build_options_pg_from_csvs yyyymm={self.yyyymm} file={p} exception={str(e)}')
        
        return df_all    


    
    def add_symbol_to_pg(self,symbol,dt_beg,dt_end):
        df = self.get_yahoo_data(symbol,dt_beg,dt_end)
        self.write_symbol_to_pg(symbol, df)

    def write_options_df_to_pg(self,df):        
        if len(df)>0:
            min_date = df.settle_date.min()
            tb_full = self.schema_name + '.' + self.options_table_name
            df_already_there = self.pga.get_sql(f"select max(settle_date) settle_date from {tb_full}  where settle_date > '{min_date}' ")
            if len(df_already_there) > 1:
                self.logger.warn(f'ERROR write_options_df_to_pg: dates are already in database')
                return
            self.logger.info(f'writing to database')
            self.pga.write_df_to_postgres_using_metadata(
                df=df,table_name=tb_full)
        else:
            raise ValueError(f'ERROR write_options_df_to_pg: Empty dataframe')        
    
    def update_daily_with_delete(self,dt_beg=None,dt_end=None):
        '''
        Update existing symbols in database by deleting data between beg and end dates first\
        :param dt_beg:
        :param dt_end:
        '''
        pga2 = self.pga
        end_date = dt_end if dt_end is not None else dt.datetime.now()
        end_date_str = end_date.strftime("%Y-%m-%d")
        beg_date = dt_beg if dt_beg is not None else end_date - dt.timedelta(self.days_to_fetch)
        beg_date_str = beg_date.strftime("%Y-%m-%d")
        sql_delete = f"""
        delete from {self.full_table_name} where date>='{beg_date_str}' and date<='{end_date_str}';
        """
        pga2.exec_sql_raw(sql_delete)
        self.update_yahoo_daily(dt_beg=dt_beg,dt_end=dt_end)
    
    def drop_underlying_table(self):
        self.pga.exec_sql_raw(f"DROP TABLE IF EXISTS  {self.schema_name}.{self.underlying_table_name};")
        
    def drop_options_table(self):
        self.pga.exec_sql_raw(f"DROP TABLE IF EXISTS  {self.schema_name}.{self.options_table_name};")

        
    def execute(self):
        if self.recreate_schema:
            self.drop_underlying_table()
            self.drop_options_table()
            self.pga.exec_sql_raw(f"DROP SCHEMA IF EXISTS  {self.schema_name};")
            self.pga.exec_sql_raw(f"create schema {self.schema_name};")

        if self.recreate_tables:
            self.recreate_pg_tables()
              
        if self.yyyymm is not None:  
            self.unzip_file()
            df_all = self.build_options_loop()
            return df_all
        
    
class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg


def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created by user_name on %s.
  Copyright 2019 organization_name. All rights reserved.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        parser.add_argument('--yyyymm',help='year month day string for day that you want to unzip and build',nargs='?')
        parser.add_argument('--commod_list',help='list of commodity contracts - separated by commas - like CL,CB,ES,GE that you want to process',nargs='?')
        parser.add_argument('--zip_folder_parent',help='top level folder that holds ACS zip files')
        parser.add_argument('--recreate_schema',help='recreate the schema for the options and underlying tables',default=False)
        parser.add_argument('--recreate_tables',help='recreate the options and underlying tables',default=False)
        parser.add_argument('--dburl',type=str,
                        help='database url (None will be localhost)',
                        nargs='?')
        parser.add_argument('--databasename',type=str,
                        help=f'databasename (None will be {BuildDb.BUILDDB_DB_NAME})',
                        nargs='?')
        parser.add_argument('--schema_name',type=str,
                        help=f'schema name for table (None will be {BuildDb.BUILDDB_SCHEMA_NAME}))',
                        nargs='?')
        parser.add_argument('--username',type=str,
                        help='username (None will be postgres)',
                        nargs='?')
        parser.add_argument('--password',type=str,
                        help='password (None will be blank)',
                        nargs='?')
        parser.add_argument('--csv_save_path',type=str,
                        help='path to use to write final dataframe to a csv file (None will be no write)',
                        nargs='?')

        # Process arguments
        args = parser.parse_args()




        yyyymm = args.yyyymm
        commod_list_string = args.commod_list
        if commod_list_string is not None:
            if "," in commod_list_string:
                commod_list = commod_list_string.split(',')
            else:
                commod_list = [commod_list_string]
        zip_folder_parent = args.zip_folder_parent
        bdb = BuildDb(zip_folder_parent,
                      yyyymm=yyyymm,
                      contract_list=commod_list,
                      dburl=args.dburl,
                      databasename=args.databasename,
                      schema_name=args.schema_name,
                      username=args.username,
                      password=args.password,
                      recreate_schema=args.recreate_schema,
                      recreate_tables=args.recreate_tables)
        df_all = bdb.execute()
        save_path = args.csv_save_path
        if save_path is not None:
            df_all.to_csv(save_path,index=False)
            
        return 0
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception as e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == "__main__":
    sys.exit(main())