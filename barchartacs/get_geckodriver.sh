# fetch geckodriver, untar it and move it to either ../ or /usr/local/bin
cd drivers
wget https://github.com/mozilla/geckodriver/releases/download/v0.21.0/geckodriver-v0.21.0-linux64.tar.gz
tar -xvzf geckodriver*
chmod +x geckodriver
echo you must now move the geckodriver to either the root of this project or to /usr/local/bin
echo sudo cp geckodriver /usr/local/bin/
echo cp geckodriver ../