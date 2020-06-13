import os
import time
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings

from hkjc_all.spiders.live_winodds import LiveWinOddsSpider
from hkjc_all.spiders.live_investment import LiveInvestmentSpider
from hkjc_all.spiders.live_qin import LiveQinSpider
from hkjc_all.spiders.live_qpl import LiveQplSpider

def crawl_and_export():

    os.system('scrapy crawl live_winodds')
    os.system('scrapy crawl live_investment')
    os.system('scrapy crawl live_qin')
    os.system('scrapy crawl live_qpl')

    os.system('mongoexport --db hkjc --collection live_winodds --out live_winodds.json')
    os.system('mongoexport --db hkjc --collection live_investment --out live_investment.json')
    os.system('mongoexport --db hkjc --collection live_qin --out live_qin.json')
    os.system('mongoexport --db hkjc --collection live_qpl --out live_qpl.json')

    os.system('gsutil cp *.json gs://tty-hr')
    os.system('rm *.json')


if __name__=="__main__":

    while True:

        crawl_and_export()
        for i in range(285):
            time.sleep(1)

