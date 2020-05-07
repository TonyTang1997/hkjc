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
        if not crawler.settings.getbool('MONGODBPIPELINE_ENABLED'):
            # if this isn't specified in settings, the pipeline will be completely disabled
            raise NotConfigured
        return cls(mongo_uri=crawler.settings.get('MONGO_URI'),mongo_db=crawler.settings.get('MONGO_DATABASE', 'items'))

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

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
        if not crawler.settings.getbool('MONGODBPIPELINE_ENABLED'):
            # if this isn't specified in settings, the pipeline will be completely disabled
            raise NotConfigured
        return cls(mongo_uri=crawler.settings.get('MONGO_URI'),mongo_db=crawler.settings.get('MONGO_DATABASE', 'items'))

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.db[self.collection_name].insert_one(dict(item))
        return item