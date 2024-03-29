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
from webdriver_manager.chrome import ChromeDriverManager
from hkjc_all.items import HkjcRaceDayItem
import re
from datetime import datetime

class RaceDaySpider(scrapy.Spider):\

    name = "racedays"

    custom_settings = {
        'ITEM_PIPELINES': {
            'hkjc_all.pipelines.HkjcAllPipeline': 400
        }
    }
    
    start_urls = [] 
    for i in range(1979,int(datetime.now().year) + 2):
        for j in ['01','02','03','04','05','06','07','09','10','11','12']:  #never have races in Aug
            tmp_urls = "https://racing.hkjc.com/racing/information/English/Racing/Fixture.aspx/?CalYear={}&CalMonth={}".format(i,j) #"/" before aspx is important to escape challenge
            start_urls.append(tmp_urls)

    current = 1

    retry_list = []

    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        self.browser = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options) #

    def parse(self, response):
        
        print("{} of {}. {}".format(self.current, len(self.start_urls), response.request.url))

        self.current += 1

        try:
            main = HkjcRaceDayItem()

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
                main['n_race'] = 14
                
                yield main

        except AttributeError:
            self.retry_list.append(str(response.request.url))
            print("retrying {} time on {}".format(self.retry_list.count(str(response.request.url)), (str(response.request.url))))
            if self.retry_list.count(str(response.request.url)) > 3:
                print("excess retry limit")
                yield main
                
            yield Request(response.url, callback = self.parse, dont_filter = True)
