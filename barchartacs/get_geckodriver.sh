# fetch geckodriver, untar it and move it to either ../ or /usr/local/bin
firefox_dir="$1"

if [[ -z ${firefox_dir} ]]
then
    firefox_dir='/opt'
fi



cd drivers
# clear out dir
rm gecko*
rm firefox*

# get geckodriver
wget https://github.com/mozilla/geckodriver/releases/download/v0.26.0/geckodriver-v0.26.0-linux64.tar.gz
tar -xvzf geckodriver*
chmod +x geckodriver
echo you must now move the geckodriver to either the root of this project or to /usr/local/bin
echo sudo cp geckodriver /usr/local/bin/
cp -n geckodriver ../

# get version 57.0 of firefox
wget http://ftp.mozilla.org/pub/firefox/releases/57.0/linux-$(uname -m)/en-US/firefox-57.0.tar.bz2
tar -xjf firefox-57.0.tar.bz2
sudo mv -vn firefox "${firefox_dir}"
sudo ln -s sudo ln -s "${firefox_dir}/firefox/firefox" /usr/bin/firefox
