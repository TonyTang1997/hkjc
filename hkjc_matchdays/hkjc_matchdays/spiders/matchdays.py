import scrapy
import os
import bs4 as bs
import requests
from lxml import html
import socket
import time
import codecs
import re
import sys
import selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

class MatchDaySpider(scrapy.Spider):
    name = "matchdays"

    start_urls = [] #"https://racing.hkjc.com/racing/information/English/Racing/Fixture.aspx?CalYear=1979&CalMonth=11"
    for i in range(1979,2021):
        for j in ['01','02','03','04','05','06','07','09','10','11','12']:  #never have races in Aug
            tmp_urls = "https://racing.hkjc.com/racing/information/English/Racing/Fixture.aspx?CalYear={}&CalMonth={}".format(i,j)
            start_urls.append(tmp_urls)

    basedict = {'date':'','veune':'','n_race':''}

    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        self.browser = webdriver.Chrome("/usr/bin/chromedriver",chrome_options=chrome_options)

    def parse(self, response):

        main = self.basedict.copy()

        self.browser.get(response.url)
        
        url = str(response.request.url)
        year = url.split("CalYear=")[1][:4]
        month = url.split("CalMonth=")[1][:2]

        #get RaceMeeting info Date and Location
        soup = bs.BeautifulSoup(self.browser.page_source, 'lxml')
        race_days = soup.find_all('td', class_='calendar')
        
        for i in race_days:
            race_day = i.find('span', class_='f_fl f_fs14').get_text().zfill(2)
            main['date'] = year+'/'+month+'/'+race_day
            main['venue'] = i.findAll('img')[0]['alt']
            main['n_race'] = len(i.findAll('img')) - 3        
            yield main
