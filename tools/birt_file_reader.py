import csv
import logging
import collections
import time
import sys

import _strptime
from datetime import datetime

from records.record import InvalidRecordProperty, InvalidRecordLength, InvalidRecord
from tools.csv_helpers import UnicodeReader

from conf import settings

class InvalidFileFormat(Exception):
    """ custom exception that is thrown when the file format is invalid """
    def __init__(self, message, *args, **kwargs):
        """ InvalidFileFormat constructor

            Parameters
            ----------
                message : str
                    A descriptive message of the error
        """
        super(InvalidFileFormat, self).__init__(message)

class BirtFileReader:
    """ the class responsible for reading a file """

    def __init__(self, provider_type, program_args, mongo_connection):
        """ BirtFileReader constructor

            Parameters
            ----------
                provider_type : object
                    A provider type object from birt_provider_type.py
                program_args: dict
                    A dict containing the argparse program arguments
        """

        self.provider_type = provider_type
        self.program_args = program_args
        self.mongo_connection = mongo_connection

        self.empty_row_count = 0 # number of empty rows encountered within record set
        self.end_of_data = None # row_count that represents that the end of the data has been reached
        self.header_row = []

    def find_header_and_end_of_data(self):
        """ find the header based off the provider_type """
        row_count = 0
        reader = UnicodeReader(self.program_args.infile, dialect=self.provider_type.dialect)
        for row_number, line in enumerate(reader):
            # store the header
            if row_number == self.provider_type.header_position:
                # store the report header and continue
                if any(field.strip() for field in line):
                    for field in line:
                        self.header_row.append(field.strip().lower())
                else:
                    raise InvalidFileFormat("header at position [%d] is empty" % row_number)
            # increment the row_count
            row_count += 1
            # check for special case where empty line signal end_of_data
            if self.provider_type.num_empty_rows_eod > 0:
                self.empty_row_count += 1
                logging.debug('empty_row_count: %d', empty_row_count)
                if self.empty_row_count >= self.provider_type.num_empty_rows_eod:
                    # store the end_of_data
                    self.end_of_data = row_count
        if self.end_of_data == None:
            # store the end_of_data
            self.end_of_data = row_count
        self.program_args.infile.seek(0) #reset file to beginning

    def process(self):
        """ process a chunk of rows in the file """
        self.find_header_and_end_of_data()

        reader = UnicodeReader(self.program_args.infile, dialect=self.provider_type.dialect)
        start = time.time()*1000.0
        count = 0
        for chunk in BirtFileReader.gen_chunks(reader, self.mongo_connection, self.program_args.verbose, self.provider_type, self.header_row, self.end_of_data):
            count += 1
            BirtFileReader.syncprocess_chunk(chunk, self.mongo_connection, self.provider_type.collection_name)
        logging.debug('all-finish: %r', (time.time()*1000.0)-start)

    @staticmethod
    def gen_chunks(reader, mongo_connection, verbose, provider_type, header_row, end_of_data):
        """ yield chunks of the file for batch processing """
        chunk = []
        for row_number, row in enumerate(reader):
            if (row_number % settings._CHUNK_SIZE == 0 and row_number > 0):
                yield chunk
                del chunk[:]
            chunk.append([row_number, row, mongo_connection, verbose, provider_type, header_row, end_of_data])
        yield chunk

    @staticmethod
    def bulk_upsert(valid_records, invalid_records, collection_name, mongo_connection):
        """ bulk upsert / inset many of the records """
        valid_result = mongo_connection.bulk_upsert(collection_name, valid_records)
        invalid_result = mongo_connection.insert_many(settings._INVALID_RECORD_COLLECTION_NAME, invalid_records)
        logging.debug('valid_result: %r', valid_result)
        logging.debug('invalid_result: %r', invalid_result)

    @staticmethod
    def syncprocess_chunk(chunk, mongo_connection, collection_name):
        # collections of valid and invaid records to be batch upsert / insert many
        valid_records = []
        invalid_records = []
        for data in chunk:
            valid, invalid = BirtFileReader.process_row(data)
            if valid != None: valid_records.append(valid)
            if invalid != None: invalid_records.append(invalid)
        BirtFileReader.bulk_upsert(valid_records, invalid_records, collection_name, mongo_connection)

    @staticmethod
    def process_row(args):
        """ process each row according to the record type contract

            Parameters
            ----------
                args[0] - row_count : int
                    the current row number
                args[1] - row: object
                    A python csv module row object
                args[2] - mongo_connection: object
                    A mongo database connection
        """
        row_count = args[0]
        row = args[1]
        mongo_connection = args[2]
        verbose = args[3]
        provider_type = args[4]
        header_row = args[5]
        end_of_data = args[6]

        if verbose:
            # echo the contents of the row in verbose mode
            print '%r' % row

        if row_count >= provider_type.data_position and row_count != end_of_data:
            if any(row):
                header_row = header_row
                provider_map = provider_type.map
                collection_name = provider_type.collection_name

                # init the record object based on the type
                try:
                    record = provider_type.record(header_row, provider_map, collection_name, row_count, mongo_connection)
                except InvalidRecordProperty as e:
                    logging.error(e.message)
                    return
                except InvalidRecordLength as e:
                    logging.error(e.message)
                    return
                except InvalidRecord as e:
                    logging.error(e.message)
                    return

                # create the record
                try:
                    record.create(row)
                except InvalidRecordProperty as e:
                    logging.error(e.message)
                    return
                except InvalidRecordLength as e:
                    logging.error(e.message)
                    return
                except InvalidRecord as e:
                    logging.error(e.message)
                    return

                # validate
                if record.validate():
                    return [record,None]
                else:
                    invalid_record = InvalidRecord(record.validation_errors(), type(record).__name__, record.row_count)
                    if invalid_record.validate():
                        return [None,invalid_record]

        return [None,None]
