import os

from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings

from hkjc_all.spiders.live_winodds import LiveWinOddsSpider

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
    d = crawl_job()
    # call schedule_next_crawl(<scrapy response>, n) after crawl job is complete
    d.addCallback(schedule_next_crawl, 290)
    d.addErrback(catch_error)

def export_to_bucket():
    os.system('mongoexport --db hkjc --collection live_winodds --out live_winodds.json')
    os.system('gsutil cp live_winodds.json gs://tty-hr')
    os.system('rm *.json')

def catch_error(failure):
    print(failure.value)

if __name__=="__main__":
    crawl()
    export_to_bucket()
    reactor.run()