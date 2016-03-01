import os
import argparse
import logging

from tools.birt_mongo import BirtMongoConnection
from tools.birt_file_reader import BirtFileReader
from records.migration import MigrationChecklistType, MigrationCoreType
from records.taxonomy import TaxonomyType

from conf import settings

class InvalidRecordType(Exception):
   """ custom exception that is thrown when attempting to parse an invalid
   record type """
   def __init__(self, message, *args, **kwargs):
        """ InvalidRecordType constructor

            Parameters
            ----------
                message : str
                    A descriptive message of the error
        """
        super(InvalidRecordType, self).__init__(message)

class MissingRecords(Exception):
    """ custom exception that is thrown when the MigrationType is run without
    an existing bird taxonomy in mongodb """
    def __init__(self, message, *args, **kwargs):
        """ MissingRecords constructor

            Parameters
            ----------
                message : str
                    A descriptive message of the error
        """
        super(MissingRecords, self).__init__(message)

class BirtConsumer(object):
    """ Command line tool to parse bird migration data """

    def __init__(self):
        self.parser = argparse.ArgumentParser(description='script to parse ' \
            'bird migration data file and populate ' \
            'a mongodb collection.')

    @staticmethod
    def file_extension(file_obj):
        """ get the file extension from a file object

            The method uses os.path.spittext to get the parts of a file
            object by its name.  The last part is the file extension

            Parameters
            ----------
                file_obj : object
                    File object

            Returns
            -------
                str
                    String value or None
        """
        parts = os.path.splitext(file_obj.name)
        ext = ''
        if len(parts) > 1:
            ext = parts[-1].lower()
        return ext

    def is_valid_file_type(self, file_obj):
        """ validate the file extension is valid

            Validation method to determine if the file_obj has a valid
            extension.  The list of valid extensions are defined in
            conf/settings.py

            Parameters
            ----------
                file_obj : object
                    File object

            Returns
            -------
                bool
                    True or False
        """
        ext = BirtConsumer.file_extension(file_obj)
        if ext not in settings._ALLOWED_FILE_EXTENSIONS:
            return False
        return True

    def add_args(self):
        """ add arguments to the argparse command-line program """
        self.parser.add_argument('-v', '--verbose',
            action="store_true",
            help="verbose output" )

        self.parser.add_argument('-t', '--type', required=True,
            choices= settings._TYPES,
            help='the type of provider to be parsed')

        self.parser.add_argument('-u', '--username',
            default=settings._MONGO_USERNAME,
            help='the username for mongoDB (Default: None)')

        self.parser.add_argument('-p', '--password',
            default=settings._MONGO_PASSWORD,
            help='the password for mongoDB (Default: None)')

        self.parser.add_argument('-d', '--database',
            default=settings._MONGO_DATABASE,
            help='the database for mongoDB (Default: birt)')

        self.parser.add_argument('-m', '--mongohost',
            default=settings._MONGO_HOST,
            help='the hostname for mongoDB (Default: localhost)')

        self.parser.add_argument('infile',
            type=argparse.FileType('rb'),
            help="the file to be parsed")

    def run(self, *args):
        """ kickoff the program """
        self.add_args()

        if len(args) > 0:
            program_args = self.parser.parse_args(args)
        else:
            program_args = self.parser.parse_args()

        # validate the filename extension
        if not self.is_valid_file_type(program_args.infile):
            msg = 'not a valid file extension %r' % settings._ALLOWED_FILE_EXTENSIONS
            self.parser.error(msg) #this calls sys.exit
        # determine the record type from the program_args
        if program_args.type == settings._TAXONOMY_TYPE:
            provider_type = TaxonomyType()
        elif program_args.type == settings._MIGRATION_CHECKLIST_TYPE:
            provider_type = MigrationChecklistType()
        elif program_args.type == settings._MIGRATION_CORE_TYPE:
            provider_type = MigrationCoreType()
        else:
            raise InvalidRecordType(program_args.type + ' is Not a valid record type')

        # setup a mongoDB connection
        #client = motor.motor_tornado.MotorClient(options.mongo_host, options.mongo_port)
        #self.db = client[options.mongo_database]
        mongo_connnection = BirtMongoConnection(program_args)

        # check if the node import has been run first
        if type(provider_type) == MigrationChecklistType:
            num_nodes = mongo_connnection.db[settings._NODE_COLLECTION_NAME].find().count()
            if num_nodes == 0:
                raise MissingRecords('Please import the type ' + settings._TAXONOMY_TYPE + ' before ' + settings._MIGRATION_CHECKLIST_TYPE)

        # create a new file reader object of the specified report type
        reader = BirtFileReader(provider_type, program_args, mongo_connnection)
        reader.process()
