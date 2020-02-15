# fetch geckodriver, untar it and move it to either ../ 
# fetch firefox, untar it and move the folder to /opt and create a symlink to /usr/bin/firefox

firefox_dir="$1"
gecko_version=$2
firefox_version=$3

# check for firefox directory
if [[ -z ${firefox_dir} ]]
then
    firefox_dir='/opt'
fi

# check for geckodriver version
if [[ -z ${gecko_version} ]]
then
    gecko_version='0.26.0'
fi

# check for firefox version
if [[ -z ${firefox_version} ]]
then
    firefox_version='57.0'
fi



cd drivers
# clear out dir
rm gecko*
rm firefox*

# get geckodriver
wget https://github.com/mozilla/geckodriver/releases/download/v${gecko_version}/geckodriver-v${gecko_version}-linux64.tar.gz
tar -xvzf geckodriver*
chmod +x geckodriver
echo you must now move the geckodriver to either the root of this project or to /usr/local/bin
echo sudo cp geckodriver /usr/local/bin/
cp -n geckodriver ../

# get version 57.0 of firefox
wget http://ftp.mozilla.org/pub/firefox/releases/${firefox_version}/linux-$(uname -m)/en-US/firefox-${firefox_version}.tar.bz2
tar -xjf firefox-57.0.tar.bz2
sudo mv -vn firefox "${firefox_dir}"
sudo ln -s "${firefox_dir}/firefox/firefox" /usr/bin/firefox
