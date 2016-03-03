import pymongo
import logging

from conf import settings

class BirtMongoConnection(object):
    """ class that contains the connection details to mongo

        Also contains common methods that are more complicated than a simple
        find_one query, such as bulk_upsert and insert_many.
    """
    @property
    def db(self):
        """ pymongo Database object """
        return self._db

    def __init__(self, program_arguments, *args, **kwargs):
        """ BirtMongoConnection constructor
            Parameters
            ----------
                program_arguments : dict
                    A dictionary of arguments collected by the argparse
                    command-line program
        """
        self._hostname = program_arguments.mongohost
        self._username = program_arguments.username
        self._password = program_arguments.password
        self._database = program_arguments.database
        self._client = None
        self._db = self.connect()
        if settings._DROP_INDEXES:
            self.drop_indexes()

    def connect(self):
        """ connect to mongoDB

            The method creates the uri to connect to the mongoDB using
            the supplied command-line arguments or default values.
        """
        if self._username != None or self._password != None:
            uri = 'mongodb://%s:%s@%s/%s?authMechanism=MONGODB-CR' % \
                (self._username, self._password, self._hostname, self._database)
        else:
            uri = 'mongodb://%s/%s' % \
                (self._hostname, self._database)
        self._client = pymongo.MongoClient(uri)
        return pymongo.database.Database(self._client, self._database)

    def drop_indexes(self):
        """ drops any existing indexes"""
        nodes = pymongo.collection.Collection(self._db, settings._NODE_COLLECTION_NAME)
        nodes.drop_indexes()
        paths = pymongo.collection.Collection(self._db, settings._PATH_COLLECTION_NAME)
        paths.drop_indexes()

    def ensure_indexes(self, *args):
        """ creates indexes on the collections if they do not exist """
        # recreates the indexes
        nodes = pymongo.collection.Collection(self._db, settings._NODE_COLLECTION_NAME)
        nodes.create_index([("loc", pymongo.GEOSPHERE)])
        nodes.create_index([
            ("_id", pymongo.ASCENDING),
            ("primary_com_name", pymongo.TEXT),
            ("species_name", pymongo.TEXT),
            ("genus_name", pymongo.TEXT),
            ("category", pymongo.TEXT),
            ("family_name", pymongo.TEXT),
            ("order_name", pymongo.TEXT),
            ("subfamily_name", pymongo.TEXT),
            ("taxon_order", pymongo.TEXT)
            ], weights={
            "taxon_order": 1,
            "subfamily_name": 2,
            "order_name": 3,
            "family_name": 4,
            "category": 5,
            "genus_name": 6,
            "species_name": 7,
            "primary_com_name": 8,
            "_id": 9
            }, name="idxTypeahead")
        paths = pymongo.collection.Collection(self._db, settings._PATH_COLLECTION_NAME)
        paths.create_index([
            ("year", pymongo.ASCENDING),
            ("month", pymongo.ASCENDING),
            ("day", pymongo.ASCENDING),
        ])
        return "Indexes have been applied."

    @staticmethod
    def format_bulk_write_results(result):
        """ BulkWriteResult object, as defined:

            http://api.mongodb.org/python/current/api/pymongo/results.html#pymongo.results.BulkWriteResult
        """
        if result == None:
            return {}

        keys = ['nInserted', 'nMatched', 'nModified', 'nRemoved', 'nUpserted']
        formatted_result = {}

        for key in keys:
            if key in result:
                formatted_result[key] = result[key]

        return formatted_result

    @staticmethod
    def format_insert_many_results(result):
        """ InsertManyResult object, as defined:

            http://api.mongodb.org/python/current/api/pymongo/results.html#pymongo.results.InsertManyResult
        """
        if result == None:
            return {}

        formatted_result = {}

        ids = result.inserted_ids
        formatted_result['nInserted'] = len(ids)

        return formatted_result

    def bulk_upsert(self, collection_name, records):
        """ bulk upsert of documents into mongodb collection

            Parameters
            ----------
                collection_name: str
                    The name of the mongoDB collection
                records: list
                    A list of record.fields.
        """
        if len(records) == 0:
            return

        collection = pymongo.collection.Collection(self._db, collection_name)
        bulk = collection.initialize_ordered_bulk_op()
        for record in records:
            bulk.find({'_id': record.id}).upsert().update({
                '$set': record.fields})

        result = None
        try:
            result = bulk.execute()
        except pymongo.errors.BulkWriteError as e:
            logging.error(e.details)

        return BirtMongoConnection.format_bulk_write_results(result)

    def insert_many(self, collection_name, records):
        """ inserts many documents into mongodb collection
            Parameters
            ----------
                collection_name: str
                    The name of the mongoDB collection
                records: list
                    A list of records.
        """
        if len(records) == 0:
            return

        record_fields = map(lambda x: x.fields, records)

        collection = pymongo.collection.Collection(self._db, collection_name)

        result = None
        try:
            result = collection.insert_many(record_fields)
        except Exception as e:
            logging.error(e)

        return BirtMongoConnection.format_insert_many_results(result)
