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
import numpy as np
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
from hkjc_all.items import HkjcLiveInvestmentItem
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
race_before_today = racedays[racedays.date <= (datetime.now() + timedelta(hours=8) - timedelta(days=1))].reset_index(drop=True)

try:
    next_raceday = racedays['date'][len(race_before_today)]
    next_race_venue = racedays['venue'][len(race_before_today)]
    
    try:
        all_race_no = len(next_racecard.race_no.unique())
    except:
        print("racecard not found")
        all_race_no = 0
        pass

except:
    print("next race not found")
    pass


class LiveInvestmentSpider(scrapy.Spider):

    name = "live_investment"
    
    custom_settings = {'ITEM_PIPELINES': {'hkjc_all.pipelines.MongoDBLiveInvestment': 400}}

    start_urls = []
    for i in range(all_race_no):
        tmp_url = "http://bet.hkjc.com/racing/getJSON.aspx/?type=pooltot&date={}&venue={}&raceno={}".format(next_raceday.strftime('%Y-%m-%d'),next_race_venue,i+1)
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

        main["time_scraped"] = (datetime.now() + timedelta(hours=8))
        main["time_updated_by_hkjc"] = str((datetime.now() + timedelta(hours=8)).strftime('%Y-%m-%d')) + "-" + str(data['updateTime'])
        main["race_date"] = next_raceday.strftime('%Y-%m-%d')
        main["venue"] = next_race_venue
        main['race_no'] = str(response.url).split('=')[-1]
        main['total_investment'] = data['totalInv']

        investment_value_dict = {x['pool'] : x['value'] for x in data['inv']}

        main['win_investment'] = investment_value_dict['WIN']
        main['win_investment'] = np.nan
        main['pla_investment'] = investment_value_dict['PLA']
        main['qin_investment'] = investment_value_dict['QIN']
        
        try:
            main['qpl_investment'] = investment_value_dict['QPL']
        except:
            main['qpl_investment'] = np.nan

        main['tce_investment'] = investment_value_dict['TCE']
        main['tri_investment'] = investment_value_dict['TRI']

        try:
            main['qtt_investment'] = investment_value_dict['QTT']
        except:
            main['qtt_investment'] = np.nan
        
        main['dbl_investment'] = investment_value_dict['DBL']
        
        yield main

