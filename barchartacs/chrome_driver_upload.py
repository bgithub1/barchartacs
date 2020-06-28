import os,sys
import requests
import io
import subprocess
import re
import xml.etree.ElementTree as ET
import xmltodict
import zipfile
    
def get_platform_architecture():
    if sys.platform.startswith('linux') and sys.maxsize > 2 ** 32:
        platform = 'linux'
        architecture = '64'
    elif sys.platform == 'darwin':
        platform = 'mac'
        architecture = '64'
    elif sys.platform.startswith('win'):
        platform = 'win'
        architecture = '32'
    else:
        raise RuntimeError('Could not determine chromedriver download URL for this platform.')
    return platform, architecture

def get_chrome_version():
    machine,bits = get_platform_architecture()
    linux_cmd = "'chromium-browser' --version"
    mac_cmd = "'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' --version"
    cmd = mac_cmd if machine=='mac' else linux_cmd
    subco = lambda c:subprocess.check_output(c,stderr=subprocess.STDOUT,shell=True)
    # r = re.findall('[0-9]{2,3}\.[0-9]\.[0-9]{2,4}\.[0-9]{2,4}',str(subco(cmd)))
    r = re.findall('[0-9]{2,3}\.[0-9]\.[0-9]{2,4}',str(subco(cmd)))
    return r[0]

def get_chrome_driver_url():
    url='https://chromedriver.storage.googleapis.com/'
    response = requests.get(url)
    sb=response.content.decode("utf-8")
    sb = sb.replace(" xmlns='http://doc.s3.amazonaws.com/2006-03-01'",'')
    my_dict = xmltodict.parse(sb)
    v = get_chrome_version()
    crit_lambda = lambda k: (f"{v}" in  k['Key']) and ('chromedriver_mac64' in k['Key']) 
    driver_zips = [k['Key'] for k in my_dict['ListBucketResult']['Contents'] if crit_lambda(k)]
    return f"{url}{driver_zips[-1]}"

def download_chromedriver(chromedriver_filename = 'chromedriver',
                          chromedriver_dir = './drivers'):
    driver_url = get_chrome_driver_url()
    driver_binary = requests.get(driver_url)
    driver_archive = io.BytesIO(driver_binary.content)
    with zipfile.ZipFile(driver_archive) as zip_file:
        zip_file.extract(chromedriver_filename, chromedriver_dir)
    chromedriver_filepath = os.path.join(chromedriver_dir, chromedriver_filename)    
    if not os.access(chromedriver_filepath, os.X_OK):
        os.chmod(chromedriver_filepath, 0o744)

