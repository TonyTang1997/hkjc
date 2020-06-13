import os

from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings

from hkjc_all.spiders.live_winodds import LiveWinOddsSpider
from hkjc_all.spiders.live_investment import LiveInvestmentSpider
from hkjc_all.spiders.live_qin import LiveQinSpider
from hkjc_all.spiders.live_qpl import LiveQplSpider

@defer.inlineCallbacks
def multi_crawl():
    """
    Job to start spiders.
    Return Deferred, which will execute after crawl has completed.
    """
    settings = get_project_settings()
    runner = CrawlerRunner(settings)
    yield runner.crawl(LiveInvestmentSpider)
    yield runner.crawl(LiveQinSpider)
    yield runner.crawl(LiveQplSpider)
    #reactor.stop()
    
def crawl_job():
    """
    Job to start spiders.
    Return Deferred, which will execute after crawl has completed.
    """
    settings = get_project_settings()
    runner = CrawlerRunner(settings)
    return runner.crawl(LiveWinOddsSpider)
    
def schedule_next_crawl(null, sleep_time):
    """
    Schedule the next crawl
    """
    reactor.callLater(sleep_time, crawl)

def crawl():
    """
    A "recursive" function that schedules a crawl 30 seconds after
    each successful crawl.
    """
    # crawl_job() returns a Deferred
    multi_crawl()
    d = crawl_job()
    # call schedule_next_crawl(<scrapy response>, n) after crawl job is complete
    d.addCallback(schedule_next_crawl, 290)
    d.addErrback(catch_error)

def export_to_bucket():
    os.system('mongoexport --db hkjc --collection live_winodds --out live_winodds.json')
    os.system('mongoexport --db hkjc --collection live_investment --out live_investment.json')
    os.system('mongoexport --db hkjc --collection live_qin --out live_qin.json')
    os.system('mongoexport --db hkjc --collection live_qpl --out live_qpl.json')

    os.system('gsutil cp *.json gs://tty-hr')
    os.system('rm *.json')

def catch_error(failure):
    print(failure.value)

if __name__=="__main__":
    crawl()
    export_to_bucket()
    reactor.run()