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
from datetime import datetime 

today = datetime.now()

home_dir = os.path.expanduser('~')
matchdays_dir = home_dir + "/matchdays.csv"
matchdays = pd.read_csv(matchdays_dir)
matchdays = matchdays.sort_values("date")

matchdays['date'] = pd.to_datetime(matchdays['date'], format='%Y/%m/%d')
matchdays = matchdays[matchdays.date < today].reset_index(drop=True)
matchdays['date'] = matchdays['date'].dt.strftime('%Y/%m/%d')

date_to_crawl = matchdays['date']
venue_to_crawl = matchdays['venue']
race_to_crawl = matchdays['n_race']

class hkRaceAllSpider(scrapy.Spider):

    name = "all"
    
    start_urls = [] 
    for i in range(len(matchdays)):
        for j in range(race_to_crawl[i]):
            tmp_urls = "https://racing.hkjc.com/racing/information/English/racing/LocalResults.aspx?RaceDate={}&Racecourse={}&RaceNo={}".format(date_to_crawl[i],venue_to_crawl[i],j+1)
            start_urls.append(tmp_urls)

    print(len(start_urls))
    
    all_headers = ['Date', 'Location','Race', 'Place', 'Number', 'Horse', 'Horse Code', 'Jockey', 'Trainer', 'Actual Wt.', 'Declar.Horse Wt.', 'Draw', 'LBW', 'Time',
    'Win Odds','Running Pos 1', 'Running Pos 2', 'Running Pos 3', 'Running Pos 4', 'Running Pos 5', 'Running Pos 6', 'Race Code', 'Class', 'Distance', 'Handicap',
    'Prize Money(HKD)', 'Condition', 'Ground', 'Course', 'Race Time 1', 'Race Time 2', 'Race Time 3', 'Race Time 4', 'Race Time 5', 'Race Time 6', 'Sectional Time 1',
    'Sectional Time 2', 'Sectional Time 3', 'Sectional Time 4', 'Sectional Time 5', 'Sectional Time 6', 'URL']

    #not sure need to scrape or not
    #,'WIN(Dividend)','PLACE(Dividend)','QUINELLA','QP1(Dividend)','QP1(Winning Combination)','QP2(Dividend)','QP2(Winning Combination)','QP3(Dividend)','QP3(Winning Combination)','TIERCE','TRIO','FIRST 4','QUARTET'

    zeroList = ['']*len(all_headers)

    baseDict = dict(zip(all_headers,zeroList))

    current = 0

    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        self.browser = webdriver.Chrome("/usr/bin/chromedriver",chrome_options=chrome_options)

    def parse(self, response):
        
        print("{} of {}. {}".format(self.current, len(self.start_urls), response.request.url))

        self.current+= 1

        main = self.baseDict.copy()

        self.browser.get(response.url)
        
        #self.crawler.engine.close_spider('log message')

        #get RaceMeeting info Date and Location
        soup = bs.BeautifulSoup(self.browser.page_source, 'lxml')

        checkblank = soup.find("div", {"id": "errorContainer"})
        if checkblank is not None:
            print("Blank")
            return

        main['URL'] = str(response.request.url)

        raceMeeting = soup.find('span', {'class': 'f_fl f_fs13'}).get_text().replace(u'\xa0', '').replace('  ',':').split(':')        
        main["Date"] = raceMeeting[1][1:]
        main["Location"] = raceMeeting[2]

        #get Race Code 
        rawRaceCode = soup.find('tr', {'class': 'bg_blue color_w font_wb'})
        
        if rawRaceCode is None:
            return
        
        raceCode = rawRaceCode.get_text().replace('\n','')
        main["Race Code"] = raceCode
        main["Race"] = raceCode.split(' ')[1]

        #get Race info
        raceInfo = soup.find('tbody', {'class': 'f_fs13'}).get_text()
        raceInfo = raceInfo.split('\n\n')
            
        #get Sectional Time
        sectionalTime = raceInfo[-2].split('\n')
        sectionalTime.pop(0)

        for i in range(len(sectionalTime)):
            main['Sectional Time '+str(i+1)] = sectionalTime[i]
    
        #get Prize Money and Race Time
        raceInfoRow3 = raceInfo[-4].split('\n')
        main["Prize Money(HKD)"] = raceInfoRow3[1][4:]
        
        raceTime = raceInfoRow3[3:]

        for i in range(len(raceTime)):  
            main['Race Time '+str(i+1)] = raceTime[i][1:-1]

        #get Course Ground Handicap
        raceInfoRow2 = raceInfo[-5].split('\n')
        main['Handicap'] = raceInfoRow2[1]

        if soup.find("td", text="Course :") is not None:
            if soup.find("td", text="Course :").findNext('td').get_text() in ['TURF','ALL WEATHER TRACK','SAND','GRASS']:
                main['Ground'] = soup.find("td", text="Course :").findNext('td').get_text()
                main['Course'] = soup.find("td", text="Course :").findNext('td').get_text()

            else:
                main['Ground'] = soup.find("td", text="Course :").findNext('td').get_text().split('"')[0][0:4]
                main['Course'] = soup.find("td", text="Course :").findNext('td').get_text().split('"')[1]
            
        #get Class Distance Condition 
        rawClassDistanceCondition = soup.find("td", text="Going :")
        main['Condition'] = rawClassDistanceCondition.findNext('td').get_text()      
        rawClassDistance = rawClassDistanceCondition.find_previous_sibling('td').get_text().split('-')
        if rawClassDistance[0] == 'GROUP':
            main['Class'] = rawClassDistance[0]+rawClassDistance[1][0]
            main['Distance'] = rawClassDistance[2][1:-2]
        else:
            main['Class'] = rawClassDistance[0][0:-1]
            main['Distance'] = rawClassDistance[1][1:-2]
            
        #get WIN PLACE QUINELLA TIERCE TRIO FRIST 4 QUARTET
        #main['QUINELLA'] = soup.find("td", text="QUINELLA").findNext('td').findNext('td').get_text()
        #QP = soup.find("td", text="QUINELLA PLACE")
        #if QP is not None:
        #    QP1 = QP.findNext('td')
        #    QP2 = QP.findNext('tr').findNext('td')
        #    QP3 = QP.findNext('tr').findNext('tr').findNext('td')
        #    if main['QUINELLA'] != 'REFUND':
        #        main['QP1(Dividend)'] = QP1.findNext('td').get_text()
        #        main['QP2(Dividend)'] = QP2.findNext('td').get_text()
        #        main['QP3(Dividend)'] = QP3.findNext('td').get_text()
        #        main['QP1(Winning Combination)'] = QP1.get_text()
        #        main['QP2(Winning Combination)'] = QP2.get_text()
        #        main['QP3(Winning Combination)'] = QP3.get_text() 
        #    else:
        #        main['QP1(Dividend)'] = 'REFUND'
        #        main['QP2(Dividend)'] = 'REFUND'
        #        main['QP3(Dividend)'] = 'REFUND'
        #        main['QP1(Winning Combination)'] = 'REFUND'
        #        main['QP2(Winning Combination)'] = 'REFUND'
        #        main['QP3(Winning Combination)'] = 'REFUND'

        #TIERCE = soup.find("td", text="TIERCE")
        #TRIO = soup.find("td", text="TRIO")
        #FIRST4 = soup.find("td", text="FIRST 4")
        #QUARTET = soup.find("td", text="QUARTET")
        #if TIERCE is not None:
        #    main['TIERCE'] = TIERCE.findNext('td').findNext('td').get_text()
        #if TRIO is not None:
        #    main['TRIO'] = TRIO.findNext('td').findNext('td').get_text()
        #if FIRST4 is not None:
        #    main['FIRST 4'] = FIRST4.findNext('td').findNext('td').get_text()
        #if QUARTET is not None:
        #    main['QUARTET'] = QUARTET.findNext('td').findNext('td').get_text()

        #get rows of the perfermance table
        table = soup.find('tbody', {'class': 'f_fs12'})
        if table is None:
            return
        trs = table.findChildren('tr')

        #placeDividend = []
        #startTr = soup.find("td", text="PLACE").find_parent('tr')
        #placeDividend.append(startTr.findNext('td').findNext('td').findNext('td').get_text())
        #while True:
        #    startTr = startTr.findNext('tr')
        #    if len(startTr) == 7:
        #        break
        #    placeDividend.append(startTr.findNext('td').findNext('td').get_text())

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
                final['Place'] = tdList[0]    
                final['Number'] = tdList[1]    
                final['Horse'] = tdList[2].split('(')[0]
                final['Horse Code'] = tdList[2].split('(')[1].replace(')','')
                final['Jockey'] = tdList[3]
                final['Trainer'] = tdList[4]
                final['Actual Wt.'] = tdList[5]
                final['Declar.Horse Wt.'] = tdList[6]
                final['Draw'] = tdList[7]
                final['LBW'] = tdList[8]
                final['Time'] = tdList[9]
                final['Win Odds'] = tdList[10]

            elif len(tdList) == 12:
                final['Place'] = tdList[0]    
                final['Number'] = tdList[1]    
                final['Horse'] = tdList[2].split('(')[0]
                final['Horse Code'] = tdList[2].split('(')[1].replace(')','')
                final['Jockey'] = tdList[3]
                final['Trainer'] = tdList[4]
                final['Actual Wt.'] = tdList[5]
                final['Declar.Horse Wt.'] = tdList[6]
                final['Draw'] = tdList[7]
                final['LBW'] = tdList[8]
                final['Time'] = tdList[10]
                final['Win Odds'] = tdList[11]
                for i in range(len(runningPosList)):
                    final['Running Pos '+str(i+1)] = runningPosList[i]
            
            #input WIN(Dividend) and PLACE(Dividend)
            #if trsIndex == 0:
            #    final['WIN(Dividend)'] = soup.find("td", text="WIN").findNext('td').findNext('td').get_text()
            #    final['PLACE(Dividend)'] = placeDividend[0]
            #if trsIndex == 1 and final['Place'] == '1DH':
            #    final['WIN(Dividend)'] = soup.find("td", text="WIN").findNext('tr').findNext('td').findNext('td').get_text()
            #if trsIndex == 1:            
            #    try:
            #        final['PLACE(Dividend)'] = placeDividend[1]
            #    except IndexError:
            #        pass
            #if trsIndex == 2:
            #    try:
            #        final['PLACE(Dividend)'] = placeDividend[2]
            #    except IndexError:
            #        pass
                
            trsIndex += 1

            yield final


