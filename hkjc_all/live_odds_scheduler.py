import os

from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings

from hkjc_all.spiders.live_winodds import LiveWinOddsSpider
from hkjc_all.spiders.live_investment import LiveInvestmentSpider
from hkjc_all.spiders.live_qin import LiveQinSpider
from hkjc_all.spiders.live_qpl import LiveQplSpider

@defer.inlineCallbacks
def crawl():
    """
    Job to start spiders.
    Return Deferred, which will execute after crawl has completed.
    """
    settings = get_project_settings()
    runner = CrawlerRunner(settings)
    yield runner.crawl(LiveWinOddsSpider)
    yield runner.crawl(LiveInvestmentSpider)
    yield runner.crawl(LiveQinSpider)
    yield runner.crawl(LiveQplSpider)
    reactor.stop()
    reactor.callLater(290, crawl)
    

def export_to_bucket():
    os.system('mongoexport --db hkjc --collection live_winodds --out live_winodds.json')
    os.system('mongoexport --db hkjc --collection live_investment --out live_investment.json')
    os.system('mongoexport --db hkjc --collection live_qin --out live_qin.json')
    os.system('mongoexport --db hkjc --collection live_qpl --out live_qpl.json')

    os.system('gsutil cp *.json gs://tty-hr')
    os.system('rm *.json')

if __name__=="__main__":
    crawl()
    export_to_bucket()
    reactor.run()