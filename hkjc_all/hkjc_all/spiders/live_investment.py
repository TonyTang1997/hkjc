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
from hkjc_all.items import HkjcLiveInvestmentItem
from scrapy import Request
from requests_html import HTMLSession, AsyncHTMLSession
import urllib.request, json 
import random

home_dir = os.path.expanduser('~')
racedays_dir = home_dir + "/hkjc/racedays.csv"
racedays = pd.read_csv(racedays_dir)
racedays = racedays.sort_values("date").reset_index(drop=True)

racedays['date'] = pd.to_datetime(racedays['date'], format='%Y/%m/%d')
race_before_today = racedays[racedays.date < datetime.now()].reset_index(drop=True)

next_raceday = racedays['date'][len(race_before_today)]
next_race_venue = racedays['venue'][len(race_before_today)]

all_race_no = racedays['n_race'][len(race_before_today)]

print(next_raceday)
print(next_race_venue)

class LiveInvestmentSpider(scrapy.Spider):

    name = "live_investment"
    
    custom_settings = {'ITEM_PIPELINES': {'hkjc_all.pipelines.MongoDBLiveInvestment': 400}}

    start_urls = []
    for i in range(all_race_no):
        tmp_url = "http://bet.hkjc.com/racing/getJSON.aspx/?type=pooltot&date={}&venue={}&raceno={}".format(next_raceday,next_race_venue,i+1)
        start_urls.append(tmp_url)

    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        self.browser = webdriver.Chrome("/usr/bin/chromedriver", chrome_options=chrome_options)
        
    def parse(self, response):

        main = HkjcLiveInvestmentItem()

        session = HTMLSession()
        r = session.get(response.url)
        data = json.loads(r.html.html)

        main["time_scaped"] = datetime.now()
        main["time_updated_by_hkjc"] = data['updateTime']
        main["race_date"] = next_raceday
        main["venue"] = next_race_venue
        main['total_investment'] = data['totalInv']

        investment_value = [x['value'] for x in data['inv']]
        
        win_investment = investment_value[0]
        pla_investment = investment_value[1]
        qin_investment = investment_value[2]
        qpl_investment = investment_value[3]
        tce_investment = investment_value[4]
        tri_investment = investment_value[5]
        qtt_investment = investment_value[7]
        dbl_investment = investment_value[8]
        
        yield main

