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
from hkjc_all.items import HkjcLiveQinItem
from scrapy import Request
from requests_html import HTMLSession, AsyncHTMLSession
import urllib.request, json 
import random
import pymongo
from pymongo import MongoClient

client = MongoClient()
db = client.hkjc
collection = db.racecard
next_racecard = pd.DataFrame(list(collection.find()))

home_dir = os.path.expanduser('~')
racedays_dir = home_dir + "/hkjc/racedays.csv"
racedays = pd.read_csv(racedays_dir)
racedays = racedays.sort_values("date").reset_index(drop=True)

racedays['date'] = pd.to_datetime(racedays['date'], format='%Y/%m/%d')
race_before_today = racedays[racedays.date < (datetime.now() + timedelta(hours=8))].reset_index(drop=True)

next_raceday = racedays['date'][len(race_before_today)]
next_race_venue = racedays['venue'][len(race_before_today)]

try:
    all_race_no = len(next_racecard.race_no.unique())
except:
    print("racecard not found")
    all_race_no = 0
    pass

class LiveQinSpider(scrapy.Spider):

    name = "live_qin"
    
    custom_settings = {'ITEM_PIPELINES': {'hkjc_all.pipelines.MongoDBLiveQin': 400}}

    start_urls = [] 
    tmp_url = "https://bet.hkjc.com/racing/getJSON.aspx/?type=qin&date={}&venue={}&raceno={}".format(next_raceday.strftime('%Y-%m-%d'),next_race_venue,all_race_no)
    start_urls.append(tmp_url)

    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        self.browser = webdriver.Chrome("/usr/bin/chromedriver", chrome_options=chrome_options)
        
        print(next_raceday)
        print(next_race_venue)

    def parse(self, response):

        main = HkjcLiveQinItem()

        session = HTMLSession()
        r = session.get(response.url)
        data = json.loads(r.html.html)

        time, odds = data['OUT'].split('@@@')[0], data['OUT'].split('@@@')[1:]

        main["time_scaped"] = (datetime.now() + timedelta(hours=8))
        main["time_updated_by_hkjc"] = str((datetime.now() + timedelta(hours=8)).strftime('%Y-%m-%d')) + "-" + str(time)
        main["race_date"] = next_raceday
        main["venue"] = next_race_venue
        main['race_no'] = str(response.url).split('=')[-1]

        qin_odds_list = odds[0].split(';')[1:]
        qin_odds = list(map(lambda x: x.split('=')[1], qin_odds_list))

        combs_dict = {91:14,78:13,66:12,55:11,45:10,36:9,28:8,21:7,15:6,10:5,6:4,3:3}

        counter  = 0

        for i in range(combs_dict[len(qin_odds)]):
            for j in range(i+1):
                main['qin_'+str(i+1)+'_'+str(j+1)] = qin_odds[counter]
                counter += 1

        yield main

