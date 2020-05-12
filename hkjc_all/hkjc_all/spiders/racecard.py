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
import pandas as pd
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
from hkjc_all.items import HkjcNewRaceItem
from scrapy import Request

tomorrow = (datetime.now() + timedelta(1)).strftime('%Y/%m/%d')

home_dir = os.path.expanduser('~')
racedays_dir = home_dir + "/hkjc/racedays.csv"
racedays = pd.read_csv(racedays_dir)
racedays = racedays.sort_values("date")

racedays['date'] = pd.to_datetime(racedays['date'], format='%Y/%m/%d')
racedays = racedays[racedays.date == tomorrow].reset_index(drop=True)
racedays['date'] = racedays['date'].dt.strftime('%Y/%m/%d')

print(tomorrow)
print(racedays)

date_to_crawl = racedays['date']
venue_to_crawl = racedays['venue']
race_to_crawl = racedays['n_race']

class RaceCardSpider(scrapy.Spider):

    name = "racecard"
    
    custom_settings = {
        'ITEM_PIPELINES': {
            'hkjc_all.pipelines.MongoDBRaceCardPipeline': 400
        }
    }

    start_urls = [] 
    for j in range(14):
        tmp_urls = "https://racing.hkjc.com/racing/Info/Meeting/RaceCard/English/Local/{}/{}/{}".format(tomorrow.strftime('%Y%m%d'),venue_to_crawl[0],j+1)
        start_urls.append(tmp_urls)

    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        self.browser = webdriver.Chrome("/usr/bin/chromedriver", chrome_options=chrome_options)
        
    def parse(self, response):

        self.browser.get(response.url)

        main = HkjcNewRaceItem()

        main["url"] = response.request.url

        main["race_date"] = response.request.url.split('/')[-3]
        main["race_no"] = response.request.url.split('/')[-1]

        #get RaceMeeting info Date and Location
        soup = bs.BeautifulSoup(self.browser.page_source, 'lxml')

        main['venue'] = soup.find('table', {'class': 'font13 lineH20 tdAlignL'}).get_text().replace(u'\xa0', '').replace(' ','').replace('\r','').split('\n')[5].split(',')[3]
        raceMeeting = soup.find('table', {'class': 'font13 lineH20 tdAlignL'}).get_text().replace(u'\xa0', '').replace(' ','').replace('\r','').split('\n')[7]

        main['race_class'] = 'Class '+raceMeeting.split(',')[-1][-1]
        if raceMeeting.split(',')[0] == 'AllWeatherTrack':

            main['condition'] = raceMeeting.split(',')[2].split('Prize')[0]
            main['track'] = 'ALL WEATHER TRACK'
            main['distance'] = raceMeeting.split(',')[1][:4]
            main['config'] = 'ALL WEATHER TRACK'
            
        else:
            main['condition'] = raceMeeting.split(',')[3].split('Prize')[0]
            main['track'] = raceMeeting.split(',')[0]
            main['distance'] = raceMeeting.split(',')[2][:4]
            main['config'] = raceMeeting.split(',')[1].replace('Course','').replace('"','')

        # get table info.
        table = soup.find('table', {'class': 'tableBorderBlue tdAlignC'}).findNext('tbody')
        if table is None:
            print("url {} is empty".format((str(response.request.url))))
            return

        trs = table.findChildren('tr')
        for tr in trs[3:]:
            final = main.copy()
            tdList = []
            for td in tr.findChildren('td'):
                tdList.append(td.get_text().replace('\n','').replace('  ',''))
            final['horse_number'] = tdList[0]    
            final['horse_name'] = tdList[3]
            final['horse_id'] = tdList[4]
            final['jockey'] = tdList[6].split('(')[0]
            final['trainer'] = tdList[9]
            final['actual_weight'] = tdList[5]
            final['declared_weight'] = tdList[12]
            final['draw'] = tdList[8]
            yield final

