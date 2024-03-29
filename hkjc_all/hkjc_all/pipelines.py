import pymongo
from scrapy.exceptions import NotConfigured

class HkjcAllPipeline(object):
    def process_item(self, item, spider):
        return item

class MongoDBHKRacePipeline(object):

    collection_name = 'hkrace'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        #if not crawler.settings.getbool('MONGODBPIPELINE_ENABLED'):
            # if this isn't specified in settings, the pipeline will be completely disabled
        #    raise NotConfigured
        return cls(mongo_uri=crawler.settings.get('MONGO_URI'),mongo_db=crawler.settings.get('MONGO_DATABASE', 'items'))

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()
        print("---finished crawling hkrace---")

    def process_item(self, item, spider):
        self.db[self.collection_name].insert_one(dict(item))
        return item

class MongoDBRaceCardPipeline(object):

    collection_name = 'racecard'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        #if not crawler.settings.getbool('MONGODBPIPELINE_ENABLED'):
            # if this isn't specified in settings, the pipeline will be completely disabled
            #raise NotConfigured
        return cls(mongo_uri=crawler.settings.get('MONGO_URI'),mongo_db=crawler.settings.get('MONGO_DATABASE', 'items'))

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()
        print("---finished crawling racecard---")

    def process_item(self, item, spider):
        self.db[self.collection_name].insert_one(dict(item))
        return item

class MongoDBLiveWinOddsPipeline(object):

    collection_name = 'live_winodds'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        #if not crawler.settings.getbool('MONGODBPIPELINE_ENABLED'):
            # if this isn't specified in settings, the pipeline will be completely disabled
            #raise NotConfigured
        return cls(mongo_uri=crawler.settings.get('MONGO_URI'),mongo_db=crawler.settings.get('MONGO_DATABASE', 'items'))

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()
        print("---finished crawling live winodds---")

    def process_item(self, item, spider):
        self.db[self.collection_name].insert_one(dict(item))
        return item

class MongoDBLiveInvestment(object):

    collection_name = 'live_investment'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        #if not crawler.settings.getbool('MONGODBPIPELINE_ENABLED'):
            # if this isn't specified in settings, the pipeline will be completely disabled
            #raise NotConfigured
        return cls(mongo_uri=crawler.settings.get('MONGO_URI'),mongo_db=crawler.settings.get('MONGO_DATABASE', 'items'))

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()
        print("---finished crawling live_investment---")

    def process_item(self, item, spider):
        self.db[self.collection_name].insert_one(dict(item))
        return item

class MongoDBLiveQin(object):

    collection_name = 'live_qin'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        #if not crawler.settings.getbool('MONGODBPIPELINE_ENABLED'):
            # if this isn't specified in settings, the pipeline will be completely disabled
            #raise NotConfigured
        return cls(mongo_uri=crawler.settings.get('MONGO_URI'),mongo_db=crawler.settings.get('MONGO_DATABASE', 'items'))

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()
        print("---finished crawling live_qin---")

    def process_item(self, item, spider):
        self.db[self.collection_name].insert_one(dict(item))
        return item

class MongoDBLiveQpl(object):

    collection_name = 'live_qpl'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        #if not crawler.settings.getbool('MONGODBPIPELINE_ENABLED'):
            # if this isn't specified in settings, the pipeline will be completely disabled
            #raise NotConfigured
        return cls(mongo_uri=crawler.settings.get('MONGO_URI'),mongo_db=crawler.settings.get('MONGO_DATABASE', 'items'))

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()
        print("---finished crawling live_qpl---")

    def process_item(self, item, spider):
        self.db[self.collection_name].insert_one(dict(item))
        return item
