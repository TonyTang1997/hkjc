# bash install command

```
sudo apt-get update

sudo apt-get -y install unzip python3-pip mongodb

sudo pip3 install --upgrade setuptools pip pyasn1-modules apache-airflow

git config --global credential.helper store
git config --global user.name "TonyTang1997"

wget -c https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb

sudo dpkg -i google-chrome-stable_current_amd64.deb

sudo apt-get -y install -f

wget https://chromedriver.storage.googleapis.com/81.0.4044.69/chromedriver_linux64.zip

unzip chromedriver_linux64.zip

sudo mv chromedriver /usr/bin/chromedriver
sudo chown root:root /usr/bin/chromedriver
sudo chmod +x /usr/bin/chromedriver

git clone https://github.com/TonyTang1997/hkjc

cd hkjc 
sudo pip3 install -r requirements.txt
```

# mongodb command 

## in mongo

```
mongo

use hkjc

db.createCollection("")
```

## in bash

```
mongoexport --db hkjc --collection live_winodds --out live_winodds.json
```

# scrapy command

```
python3 -m scrapy crawl PROJECT 

python3 -m scrapy crawl PROJECT -o 

```

