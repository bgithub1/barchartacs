'''
************************ Overview ************************
This workbook automates the download of options and futures daily pricing 
  zip files from Barchart ACS.

1. It will use Selenium to login to the Barchart ACS site; 
2. It will scrape the urls of the zip files, and then download those zipfiles into a folder which is specified using:
    folder (specified by the command line argumet zip_folder_parent)
 
************************ Usage ************************
python3 step_01_download_monthly_acs_files.py --acs_username myusername \
--acs_password mypassword --zip_folder_parent myhome/barchart_files  --begin_yy 11 --end_yy 19


'''

'''
************************** Step 1:  Imports **************************
'''
import argparse as ap
import sel_scrape as sc #@UnresolvedImport
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
import requests
from requests.auth import HTTPBasicAuth
import os
import time
import traceback
from tqdm import tqdm
import numpy as np
import logging
import re


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

if __name__ == '__main__':
    parser =  ap.ArgumentParser()
    parser.add_argument('--acs_username',type=str,
                        help='username of Barchart ACS service')
    parser.add_argument('--acs_password',type=str,
                        help='password of Barchart ACS service')
    parser.add_argument('--zip_folder_parent',type=str,
                        help='full folder path into which you will download zip files')
    parser.add_argument('--begin_yy',type=int,
                        help='2 character year like 11 for 2011 or 23 for 2023 for first year of options zip files to download from barchart')
    parser.add_argument('--end_yy',type=int,
                        help='2 character year like 11 for 2011 or 23 for 2023 for last year of options zip files to download from barchart')
    parser.add_argument('--month_list',type=str,
                        help='comma separated list (NO SPACES) of 3 character months like jan,feb,mar.  Omit to do all months for each year.',
                        default="")
    parser.add_argument('--log_file_path',type=str,
                        help='path to log file. Default = logfile.log',
                        default = 'logfile.log')
    parser.add_argument('--show_browser',type=bool,
                        help='if --show_browser is on the command line, then the browser will be shown during scraping')
    parser.add_argument('--logging_level',type=str,
                        help='log level.  Default = INFO',
                        default = 'INFO')
    parser.add_argument('--geckodriver_path',type=str,
                        help='location of geckodriver',
                        default = './geckodriver')
    
    
    args = parser.parse_args()

    '''
    ********************************** Step 2: Set import variables ******************
    
    Determine years to download, and the download location
    
    Set the variables 
        ZIP_FOLDER_PARENT
        BEGIN_YY
        END_YY
     
    These values determine 
    1. The location to which zip files get downloaded;
    2. The first year and last year of daily options settlements to scrape from the Barchart ACS website.
    
    '''
    
    ZIP_FOLDER_PARENT = args.zip_folder_parent
    BEGIN_YY = args.begin_yy
    END_YY = args.end_yy
    ACS_USERNAME = args.acs_username
    ACS_PASSWORD = args.acs_password
    month_list = args.month_list
    
    log_file_path = args.log_file_path
    logging_level = args.logging_level
    logger = init_root_logger(log_file_path, logging_level)
    
    logger.info(f'ZIP_FOLDER_PARENT into which files will be download = {ZIP_FOLDER_PARENT}')
    headless = args.show_browser
    
    
    '''
    **************************** Step 3: Set important constants *****************************
    The constants below should be left as is - DO NOT CHANGE.
    '''
    MMM_LIST = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    if month_list is not None and len(month_list)>0:
        MMM_LIST = month_list.replace(" ","").split(",")
    logger.info(f"using month list {MMM_LIST}")
    YY_LIST = list(np.arange(BEGIN_YY,END_YY+1))
    MMMYY_LIST = [mmm + str(yy) for mmm in MMM_LIST for yy in YY_LIST]
    ACS_HOME_PAGE = 'http://acs.barchart.com/mri/mripag.htm' 
#     ACS_FUTURES_PAGE = 'http://acs.barchart.com/mri/mridta.htm'
    ACS_FUTURES_PAGE = 'http://acs.barchart.com/mri/mrgfutz.htm'
    ACS_OPTIONS_PAGE = 'http://acs.barchart.com/mri/mriopt.htm'
    
    '''
    **************************** Step 4: Set ZIP_FOLDER_PARENT ******************************
    Determine ZIP_FOLDER_PARENT, which represents the folder into which Barchart ACS zip files get downloaded.
    '''
    if not os.path.exists(ZIP_FOLDER_PARENT):
        logger.info(f'making parent folder {ZIP_FOLDER_PARENT}')
        os.makedirs(ZIP_FOLDER_PARENT)
    else:
        logger.info(f'parent folder {ZIP_FOLDER_PARENT} already exists')
    

    '''
    ************************ Step 5: Instantiate SelScape ***************
    Instantiate an instance of sel_scape.SelScrape in order to 
    scrape the Barchart ACS website. 
    '''
    
#     sela = sc.SelScrape(headless=headless)
    sela = sc.SelScrape(headless=headless,executable_path=args.geckodriver_path)
    
    sela.goto(ACS_HOME_PAGE)
    time.sleep(1)
    wait(sela.driver, 5).until(EC.alert_is_present())
    alert = sela.driver.switch_to_alert()
    alert.send_keys(f'{ACS_USERNAME}{Keys.TAB}{ACS_PASSWORD}')
    time.sleep(3)
    alert.accept()
    
    '''
    ************************ Step 6: Navigate to Home Page ****************
    '''
    sela.goto(ACS_OPTIONS_PAGE)
    
    
    '''
    ************************ Step 7: Obtain URLS ****************
    Scrape the urls for options zip files to be downloaded.
    '''
    monthly_csv_files_xpath = "//a[contains(@href,'data/opt/opv')]"
    mcsv_elements = sela.findxpath(monthly_csv_files_xpath)['value']
    mcsv_hrefs_all = []
    for mcsv in mcsv_elements:
        mcsv_hrefs_all.append(mcsv.get_attribute('href'))
    mcsv_hrefs_all
    
    def is_valid_yyymm(h):
        return any([m in h for m in MMMYY_LIST])
    mcsv_hrefs = [h for h in mcsv_hrefs_all if is_valid_yyymm(h) ] 
    mcsv_hrefs
    
    
    '''
    ************************ Step 8:  Execute Download ****************
    Download the zip files into their appropriate folders.
    '''
    
    options_parent = ZIP_FOLDER_PARENT+'/options'
    if not os.path.isdir(options_parent):
        logger.info(f'making options folder {options_parent}')
        os.mkdir(options_parent)
    else:
        logger.info(f'options folder {options_parent} already exists')
    hrefs_to_unzip = []
    paths_to_unzip_to = []
    for mcsv_href in mcsv_hrefs:
        zip_file_name = mcsv_href.split('/')[-1]
        folder_name = zip_file_name.replace('.zip','')
        path_to_zip_folder = f'{options_parent}/{folder_name}'
        if not os.path.isdir(path_to_zip_folder):
            logger.info(f'making {path_to_zip_folder}')
            os.mkdir(path_to_zip_folder)
        path_to_zip_file = f'{path_to_zip_folder}/{zip_file_name}'
        if not os.path.isfile(path_to_zip_file):
            hrefs_to_unzip.append(mcsv_href)
            paths_to_unzip_to.append(path_to_zip_file)
    
    successful_downloads = []
    for i in tqdm(range(len(hrefs_to_unzip))):
        url = hrefs_to_unzip[i]
        path_to_zip_file = paths_to_unzip_to[i]
        try:    
            r=requests.get(url, auth=HTTPBasicAuth(ACS_USERNAME, ACS_PASSWORD))
            p = paths_to_unzip_to[i]
            with open(p, 'wb') as f:
                f.write(r.content)
            successful_downloads.append(path_to_zip_file)
        except Exception as e:
            traceback.print_exc()
    
    sela.driver.quit()        
    
    '''
    ************************ Step 9:  Setup Folders for Futures Download ****************
    '''
    futures_parent = ZIP_FOLDER_PARENT+'/futures'
    if not os.path.isdir(futures_parent):
        logger.info(f'making futures folder {futures_parent}')
        os.mkdir(futures_parent)
    else:
        logger.info(f'futures folder {futures_parent} already exists')


    '''
    ************************ Step 10:  Instantiate a new SelScrape ****************
    '''
    sela = sc.SelScrape(headless=headless)
    sela.goto(ACS_HOME_PAGE)
    time.sleep(1)
    wait(sela.driver, 5).until(EC.alert_is_present())
    alert = sela.driver.switch_to_alert()
    alert.send_keys(f'{ACS_USERNAME}{Keys.TAB}{ACS_PASSWORD}')
    time.sleep(3)
    alert.accept()

    '''
    ************************ Step 11:  Execute the Download of Monthly Futures zip files ****************
    '''
    sela.goto(ACS_FUTURES_PAGE)
    monthly_csv_files_xpath = "//td/a[contains(@href,'data/mrg/mrg')]"
    mcsv_elements = sela.findxpath(monthly_csv_files_xpath)['value']
    mcsv_hrefs_all = []
    for mcsv in mcsv_elements:
        mcsv_hrefs_all.append(mcsv.get_attribute('href'))
    all_years = np.arange(BEGIN_YY,END_YY+1)
    mcsv_hrefs = [h for h in mcsv_hrefs_all if (int(re.findall('[0-9]{1,2}',h)[0]) in all_years) and  (is_valid_yyymm(h))]    
    
    hrefs_to_unzip = []
    paths_to_unzip_to = []
    for mcsv_href in mcsv_hrefs:
        zip_file_name = mcsv_href.split('/')[-1]
        path_to_zip_file = f'{futures_parent}/{zip_file_name}'
        if not os.path.isfile(path_to_zip_file):
            hrefs_to_unzip.append(mcsv_href)
            paths_to_unzip_to.append(path_to_zip_file)
            
    successful_downloads = []
    for i in tqdm(range(len(hrefs_to_unzip))):
        try:    
            url = hrefs_to_unzip[i]
            r=requests.get(url, auth=HTTPBasicAuth(ACS_USERNAME, ACS_PASSWORD))
            p = paths_to_unzip_to[i]
            with open(p, 'wb') as f:
                f.write(r.content)
            successful_downloads.append(path_to_zip_file)
        except Exception as e:
            traceback.print_exc()
            
            
    sela.driver.quit() 
    
    '''
    ************************ END ****************
    '''
           
        
