'''
Created on May 06, 2019

@author: bperlman1
'''

import urllib.request as ur
from selenium import webdriver
# from selenium.webdriver.firefox.webdriver import WebDriver as fire_driver
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
# from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import traceback
import time
import logging

import inspect 
import argparse as ap
import os

def get_full_path_of_import(import_module_reference):
    """
    GET THE FOLDER THAT HOLDS THE MODULE import_module_reference
    Example: (see bankprocessing qba)
    import machine.mach
    import pypg.pg_pandas as pg
    folder_of_mach = pg.get_full_path_of_import(pg)
    
    :param import_module_reference: a reference (NOT THE STRING REFERENCE) to an imported module
    """
#     path_split = inspect.getfile(import_module_reference).split("/")
    path_split = os.path.split(inspect.getfile(import_module_reference))
#     ret_path = "/".join(path_split[:len(path_split)-1])
    ret_path = os.path.join(path_split[:len(path_split)-1])
    return ret_path


def init_root_logger(logfile,logging_level=None):
    level = logging_level
    if level is None:
        level = logging.DEBUG
    # get root level logger
    logger = logging.getLogger()
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

def gpar():
    """
    Get a basic argument parser object with a logger and log level argument added to it
    """
    parser = ap.ArgumentParser()

    parser.add_argument('--logfile_path', type=str, help="path to a logfile", default='logfile.log')
    parser.add_argument('--logging_level', type=str, help="log level for logger", default='INFO')
    return parser 

def lfa(args):
    """
    Logger From Args
    Get a root logger using li.init_root_logger and the args from parser.parser_args()
    :param args:
    """
    logger = init_root_logger(args.logfile_path, args.logging_level)
    return logger


class SelScrape(object):
    '''
    Simplify access to various selenium features like finding elements by xpath, 
       clicking on elements and extracting data from elements
    '''
    def __init__(self,headless=None,driver_name=None,profile_path=None,
                 exclude_list=None,
                 executable_path=None,
                 download_folder=None,
                 firefox_binary_path=None,
                 download_types=None):
        '''
        
        :param headless: if False, show browser. Default is True
        :param driver_name: one of chrome, chrome_linux, firefox,firefox_from_binary. Default is firefox. 
        :param profile_path:
        :param exclude_list:
        :param executable_path:
        :param download_folder:
        :param firefox_binary_path:
        :param download_types:
        '''
        
        self.headless=headless
        self.driver_name=driver_name

        if download_types is not None:
            self.download_types = download_types
        else:
            self.download_types = 'pdf'

            
            
        self.profile_path = profile_path
        self.exclude_list = exclude_list
        self.executable_path = executable_path
        
#         self.this_folder = get_full_path_of_import(dum)
        if driver_name is None:
            self.driver = self._firefox_driver()
        elif driver_name.lower() == 'chrome_linux':
            if self.executable_path is None:
                self.executable_path = self.this_folder + "/drivers/chromedriver_linux"
            self.driver = self._chrome_driver()
        elif driver_name.lower() == 'chrome':
            if self.executable_path is None:
                self.executable_path = self.this_folder + "/drivers/chromedriver"
            self.driver = self._chrome_driver()
        elif driver_name.lower() == 'firefox_from_binary':
            self.driver = self._firefox_with_binary(download_folder, firefox_binary_path)
        else:
            self.driver = self._firefox_driver()
            
    def _firefox_driver(self):
        options = webdriver.FirefoxOptions()
        h = self.headless
        if h is None:
            options.add_argument('-headless')
        else:
            if str(h).lower()=='true':
                options.add_argument('-headless') 
        if self.profile_path is not None:
            options.add_argument('--user-data-dir=%s' %(self.profile_path))
        if self.exclude_list is not None:
            options.add_experimental_option('excludeSwitches', self.exclude_list)
        driver=None        
        if self.executable_path is not None:
            driver = webdriver.Firefox(executable_path=self.executable_path,options=options)
        else:
            driver = webdriver.Firefox(options=options)

        return driver

    def _chrome_driver(self):
        options = webdriver.ChromeOptions()
        h = self.headless
        if h is None:
            options.add_argument('-headless')
        else:
            if str(h).lower()=='true':
                options.add_argument('-headless') 
        if self.profile_path is not None:
            options.add_argument('--user-data-dir=%s' %(self.profile_path))
        if self.exclude_list is not None:
            options.add_experimental_option('excludeSwitches', self.exclude_list)
        options.add_argument('--dns-prefetch-disable')
#         options.add_argument('--lang=en-US')
        driver=None
        if self.executable_path is not None:
            driver = webdriver.Chrome(executable_path=self.executable_path,options=options)
        else:
            driver = webdriver.Chrome(options=options)
        return driver

    def _firefox_with_binary(self,download_folder,firefox_binary_path=None):
        if self.profile_path is None:
            profile = webdriver.FirefoxProfile()
            profile.set_preference("browser.download.folderList", 2)
            profile.set_preference("browser.download.manager.showWhenStarting",False);
            profile.set_preference("browser.download.dir", download_folder)
            profile.set_preference("browser.download.useDownloadDir", True)
            profile.set_preference("browser.download.manager.useWindow", False);        
            profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/pdf")
        else:
            profile = webdriver.FirefoxProfile(self.profile_path)
            profile.set_preference("browser.download.folderList", 2)
            profile.set_preference("browser.download.manager.showWhenStarting",False);
            profile.set_preference("browser.download.dir", download_folder)
        options = webdriver.FirefoxOptions()
        h = self.headless
        if h is None:
            options.add_argument('-headless')
        else:
            if str(h).lower()=='true':
                options.add_argument('-headless') 


        if firefox_binary_path is None:
            driver = webdriver.Firefox(firefox_profile=profile,options=options,executable_path=self.executable_path)
        else:
            binary = FirefoxBinary(firefox_binary_path)
            driver = webdriver.Firefox(firefox_profile=profile, firefox_binary=binary,options=options,executable_path=self.executable_path)
        return driver

    def download_file_from_href(self,xpath,destination_path):
        if 'http' == xpath[:4]:
            download_url = xpath
        else:
            download_url = self.findxpath(xpath)['value'][0].get_attribute('href')
        all_cookies = self.driver.get_cookies()
        
        opener = ur.build_opener()
        cookies = {}  
        for s_cookie in all_cookies:
            cookies[s_cookie["name"]]=s_cookie["value"]
            cookie_string = '%s=%s' %(s_cookie["name"],s_cookie["value"])
            opener.addheaders.append(('Cookie',cookie_string))

        f = open(destination_path, 'w')
        response = opener.open(download_url)
        f.write(response.read())
        f.close()


    
    def goto(self,url):
        # try several times
        driver_error = None
        for i in range(0,3):
            try:
                self.driver.get(url)
                driver_error = None
                break
            except Exception as e:
                driver_error  = e
                time.sleep(3)
        if driver_error is not None:
            raise ValueError(str(driver_error) + ' url: ' + url)
        
    def sendkeys(self,xpath,value,elem_index=None):
        try:
            d = self.findxpath(xpath)
            if d['value'] is None:
                return d
            elem = d['value']
            ei = elem_index
            if ei is None:
                ei = 0
            elem[ei].send_keys(str(value))
            return {'status':None,'value':0}
        except Exception as e:
            traceback.print_exc()
            return {'status':e,'value':-1} 
    
    def href_click_element(self,xpath,elem_index=None):
        pass
    
    def click_element(self,xpath,elem_index=None):
        try:
            d = self.findxpath(xpath)
            if d['value'] is None:
                return d
            ei = elem_index
            if ei is None:
                ei = 0
            elem = d['value']
            elem[ei].click()
            return {'status':None,'value':0}
        except Exception as e:
            traceback.print_exc()
            return {'status':e,'value':-1} 

    def select_element(self,xpath,value_to_select,elem_index=None):
        try:
            el = self.findxpath(xpath)
            if el['status'] is not None:
                raise ValueError(str(el['status']))
            elem_to_select = el['value'][0]
            elem = Select(elem_to_select)
            elem.select_by_visible_text(value_to_select)
            return {'status':None,'value':0}
        except Exception as e:
            traceback.print_exc()
            return {'status':e,'value':-1} 
        

    def findxpath(self,xpath):
        try:
            elem = self.driver.find_elements_by_xpath(xpath)
            if len(elem)<=0:
                return {'status':'not found','value':None}
            return {'status':None,'value':elem}
        except Exception as e:
            traceback.print_exc()
            return {'status':e,'value':None} 
    
    def wait_for_element(self,xpath,max_seconds,result_xpath=None):
        try:
            rxp = result_xpath
            if rxp is None:
                rxp = xpath 
            elem = self.findxpath(xpath)['value']
            if elem is None:
                return -1
            elem_to_wait_for = WebDriverWait(self.driver, max_seconds).until(
                EC.presence_of_element_located((By.XPATH, rxp)))
            if elem_to_wait_for is None:
                return -1
            return 0
        except Exception as e:
            traceback.print_exc()
            return {'status':e,'value':None} 
    
    def wait_implicitly(self,seconds_to_wait):
        try:
            self.driver.implicitly_wait(int(str(seconds_to_wait)))
            return {'status':None,'value':'0'}
        except Exception as e:
            traceback.print_exc()
            return {'status':e,'value':None} 
    
    def curr_url(self):
        return self.driver.current_url
    
             
class SelDictAccess(object):
    '''
    Access selenium via sel_scrape using a dictionary of xpaths
    '''
    def __init__(self,
                 dict_xpath,main_url,
                 sac=None,logger=None):
        self.sac = sac if sac is not None else SelScrape()
        self.logger = logger if logger is not None else init_root_logger('logfile.log', 'INFO')
        self.main_url = main_url
        self.dict_xpath = dict_xpath
        self.main_home()
    
    def main_home(self):
        self.sac.goto(self.main_url)
        
    def enter_element(self,key,value,dict_replacement=None):
        xpath = self.dict_xpath[key]
        if dict_replacement is not None:
            xpath = xpath %(dict_replacement)
        d = self.sac.sendkeys(xpath,value)
        return d
    
    def click_element(self,key,dict_replacement=None):
        xpath = self.dict_xpath[key]
        if dict_replacement is not None:
            xpath = xpath %(dict_replacement)
        d = self.sac.click_element(xpath)
        return d

    
    def find_xpath(self,key,dict_replacement=None):
        xpath = self.dict_xpath[key]
        if dict_replacement is not None:
            xpath = xpath %(dict_replacement)
        r = self.sac.findxpath(xpath)
        if len(r)<=0:
            return None
        return r

    def cur_url(self):
        return self.sac.curr_url()
    
