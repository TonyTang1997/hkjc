```
sudo apt-get update

sudo apt-get install unzip python3-pip

pip3 install --upgrade setuptools
pip3 install --upgrade pip

pip install --upgrade pyasn1-modules

git config --global credential.helper store
git config --global user.name "TonyTang1997"

wget -c https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb

sudo dpkg -i google-chrome-stable_current_amd64.deb

sudo apt-get install -f

wget https://chromedriver.storage.googleapis.com/81.0.4044.69/chromedriver_linux64.zip

unzip chromedriver_linux64.zip

sudo mv chromedriver /usr/bin/chromedriver
sudo chown root:root /usr/bin/chromedriver
sudo chmod +x /usr/bin/chromedriver

cd hkjc 
pip3 install -r requirements.txt
```
