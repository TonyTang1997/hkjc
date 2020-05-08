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
from hkjc_all.items import HkjcAllItem
from scrapy import Request

yesterday = (datetime.now() - timedelta(1))

home_dir = os.path.expanduser('~')
racedays_dir = home_dir + "/hkjc/racedays.csv"
racedays = pd.read_csv(racedays_dir)
racedays = racedays.sort_values("date")

racedays['date'] = pd.to_datetime(racedays['date'], format='%Y/%m/%d')
racedays = racedays[racedays.date == yesterday].reset_index(drop=True)
racedays['date'] = racedays['date'].dt.strftime('%Y/%m/%d')

date_to_crawl = racedays['date']
venue_to_crawl = racedays['venue']
race_to_crawl = racedays['n_race']

class hkRaceAllSpider(scrapy.Spider):

    name = "recent"
    
    custom_settings = {
        'ITEM_PIPELINES': {
            'hkjc_all.pipelines.MongoDBHKRacePipeline': 400
        }
    }

    start_urls = [] 
    for i in range(len(racedays)):
        for j in range(race_to_crawl[i]):
            tmp_urls = "https://racing.hkjc.com/racing/information/English/racing/LocalResults.aspx/?RaceDate={}&Racecourse={}&RaceNo={}".format(date_to_crawl[i],venue_to_crawl[i],j+1)
            start_urls.append(tmp_urls)

    print(len(start_urls))
    
    current = 1
    
    retry_list = []
    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        self.browser = webdriver.Chrome("/usr/bin/chromedriver", chrome_options=chrome_options)

    def parse(self, response):
        
        print("{} of {}. {}".format(self.current, len(self.start_urls), response.request.url))

        self.current += 1

        try:
            main = HkjcAllItem()

            self.browser.get(response.url)

            #get RaceMeeting info Date and Location
            soup = bs.BeautifulSoup(self.browser.page_source, 'lxml')

            main['url'] = str(response.request.url)

            raceMeeting = soup.find('span', {'class': 'f_fl f_fs13'}).get_text().replace(u'\xa0', '').replace('  ',':').split(':')

            main["race_date"] = raceMeeting[1][1:]
            main["venue"] = raceMeeting[2]


            #get Race Code 
            rawRaceCode = soup.find('tr', {'class': 'bg_blue color_w font_wb'})
            
            raceCode = rawRaceCode.get_text().replace('\n','')
            main["race_code"] = raceCode
            main["race_no"] = raceCode.split(' ')[1]

            #get Race info
            raceInfo = soup.find('tbody', {'class': 'f_fs13'}).get_text()
            raceInfo = raceInfo.split('\n\n')
                
            #get Sectional Time
            sectionalTime = raceInfo[-2].split('\n')
            sectionalTime.pop(0)

            for i in range(len(sectionalTime)):
                main['sectional_time_'+str(i+1)] = sectionalTime[i]
        
            #get Prize Money and Race Time
            raceInfoRow3 = raceInfo[-4].split('\n')
            main["prize_money"] = raceInfoRow3[1][4:]
            
            raceTime = raceInfoRow3[3:]

            for i in range(len(raceTime)):  
                main['race_time_'+str(i+1)] = raceTime[i][1:-1]

            #get Course Ground Handicap
            raceInfoRow2 = raceInfo[-5].split('\n')
            main['handicap'] = raceInfoRow2[1]

            if soup.find("td", text="Course :") is not None:
                if soup.find("td", text="Course :").findNext('td').get_text() in ['TURF','ALL WEATHER TRACK','SAND','GRASS','EQUITRACK']:
                    main['track'] = soup.find("td", text="Course :").findNext('td').get_text()
                    main['config'] = soup.find("td", text="Course :").findNext('td').get_text()

                else:
                    main['track'] = soup.find("td", text="Course :").findNext('td').get_text().split('"')[0][0:4]
                    main['config'] = soup.find("td", text="Course :").findNext('td').get_text().split('"')[1]
                
            #get Class Distance Condition 
            rawClassDistanceCondition = soup.find("td", text="Going :")
            main['condition'] = rawClassDistanceCondition.findNext('td').get_text()      
            rawClassDistance = rawClassDistanceCondition.find_previous_sibling('td').get_text().split('-')
            if rawClassDistance[0] == 'GROUP':
                main['race_class'] = rawClassDistance[0]+rawClassDistance[1][0]
                main['distance'] = rawClassDistance[2][1:-2]
            else:
                main['race_class'] = rawClassDistance[0][0:-1]
                main['distance'] = rawClassDistance[1][1:-2]
                
            #get WIN PLACE QUINELLA TIERCE TRIO FRIST 4 QUARTET
            QUINELLA = soup.find("td", text="QUINELLA")
            if QUINELLA is not None:
                main['quinella'] = QUINELLA.findNext('td').findNext('td').get_text()
            QP = soup.find("td", text="QUINELLA PLACE")
            if QP is not None:
                QP1 = QP.findNext('td')
                QP2 = QP.findNext('tr').findNext('td')
                QP3 = QP.findNext('tr').findNext('tr').findNext('td')
                if main['quinella'] != 'REFUND':
                    main['qp1_dividend'] = QP1.findNext('td').get_text()
                    main['qp2_dividend'] = QP2.findNext('td').get_text()
                    main['qp3_dividend'] = QP3.findNext('td').get_text()
                    main['qp1_win_com'] = QP1.get_text()
                    main['qp2_win_com'] = QP2.get_text()
                    main['qp3_win_com'] = QP3.get_text() 
                else:
                    main['qp1_dividend'] = 'REFUND'
                    main['qp2_dividend'] = 'REFUND'
                    main['qp3_dividend'] = 'REFUND'
                    main['qp1_win_com'] = 'REFUND'
                    main['qp2_win_com'] = 'REFUND'
                    main['qp3_win_com'] = 'REFUND'

            TIERCE = soup.find("td", text="TIERCE")
            TRIO = soup.find("td", text="TRIO")
            FIRST4 = soup.find("td", text="FIRST 4")
            QUARTET = soup.find("td", text="QUARTET")
            if TIERCE is not None:
                main['tierce'] = TIERCE.findNext('td').findNext('td').get_text()
            if TRIO is not None:
                main['trio'] = TRIO.findNext('td').findNext('td').get_text()
            if FIRST4 is not None:
                main['first_4'] = FIRST4.findNext('td').findNext('td').get_text()
            if QUARTET is not None:
                main['quartet'] = QUARTET.findNext('td').findNext('td').get_text()

            #get rows of the perfermance table
            table = soup.find('tbody', {'class': 'f_fs12'})
            if table is None:
                return
            trs = table.findChildren('tr')

            PLACE = soup.find("td", text="PLACE")        

            if PLACE is not None:
                placeDividend = []
                startTr = PLACE.find_parent('tr')
                placeDividend.append(startTr.findNext('td').findNext('td').findNext('td').get_text())
                while True:
                    startTr = startTr.findNext('tr')
                    if len(startTr) == 7:
                        break
                    placeDividend.append(startTr.findNext('td').findNext('td').get_text())

            trsIndex = 0

            for tr in trs:
                
                final = main.copy()
                tdList = []
                rawRunningPosList =[]

                for td in tr.findChildren('td'):
                    tdList.append(td.get_text().replace('\n','').replace('  ',''))

                for td in tr.findChildren('td'):
                    rawRunningPosList.append(td.get_text().replace('\n\n',':').replace('\n','').replace('  ',''))
                runningPosList = rawRunningPosList[9].split(':')
                runningPosList = runningPosList[1:-1]

                if len(tdList) == 11:
                    final['result'] = tdList[0]    
                    final['horse_number'] = tdList[1]    
                    final['horse_name'] = tdList[2].split('(')[0]
                    final['horse_id'] = tdList[2].split('(')[1].replace(')','')
                    final['jockey'] = tdList[3]
                    final['trainer'] = tdList[4]
                    final['actual_weight'] = tdList[5]
                    final['declared_weight'] = tdList[6]
                    final['draw'] = tdList[7]
                    final['LBW'] = tdList[8]
                    final['finish_time'] = tdList[9]
                    final['win_odds'] = tdList[10]

                elif len(tdList) == 12:
                    final['result'] = tdList[0]    
                    final['horse_number'] = tdList[1]    
                    final['horse_name'] = tdList[2].split('(')[0]
                    final['horse_id'] = tdList[2].split('(')[1].replace(')','')
                    final['jockey'] = tdList[3]
                    final['trainer'] = tdList[4]
                    final['actual_weight'] = tdList[5]
                    final['declared_weight'] = tdList[6]
                    final['draw'] = tdList[7]
                    final['LBW'] = tdList[8]
                    final['finish_time'] = tdList[10]
                    final['win_odds'] = tdList[11]
                    for i in range(len(runningPosList)):
                        final['running_pos_'+str(i+1)] = runningPosList[i]
                
                #input WIN(Dividend) and PLACE(Dividend)
                WIN = soup.find("td", text="WIN")
                
                if WIN is not None:
                    if trsIndex == 0:
                        final['win_dividend'] = WIN.findNext('td').findNext('td').get_text()
                        final['place_dividend'] = placeDividend[0]
                    #if trsIndex == 1 and main['result'] == '1DH':
                    #    final['win_dividend'] = WIN.findNext('tr').findNext('td').findNext('td').get_text()
                    if trsIndex == 1:            
                        try:
                            final['place_dividend'] = placeDividend[1]
                        except IndexError:
                            pass
                    if trsIndex == 2:
                        try:
                            final['place_dividend'] = placeDividend[2]
                        except IndexError:
                            pass
                    
                trsIndex += 1

                yield final

        except AttributeError:
            self.retry_list.append(str(response.request.url))
            print("retrying {} time on {}".format(self.retry_list.count(str(response.request.url)), (str(response.request.url))))
            if self.retry_list.count(str(response.request.url)) > 3:
                print("excess retry limit")
                main["race_date"]  = "blank"
                main["venue"] = "blank"
                return main
            yield Request(response.url, callback = self.parse, dont_filter = True)
