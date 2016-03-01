import logging

from cerberus import Validator
from conf import settings

from records.record import InvalidRecord, InvalidRecordLength, InvalidRecordProperty, Record
from tools.csv_helpers import TabDialect, CommaDialect

class TaxonomyType(object):
    """ class that represents the .csv format of a birt node """

    @property
    def map(self):
        return { 'sci_name': { 'maps_to': 'sci_name'},
        'taxon_order': { 'maps_to': 'taxon_order'},
        'primary_com_name': { 'maps_to': 'primary_com_name'},
        'category': { 'maps_to': 'category'},
        'order_name': { 'maps_to': 'order_name'},
        'family_name':{ 'maps_to': 'family_name'},
        'subfamily_name': { 'maps_to': 'subfamily_name'},
        'genus_name': { 'maps_to': 'genus_name'},
        'species_name': { 'maps_to': 'species_name'}}

    def __init__(self):
        """ NodeType constructor

        Describes the 'contract' for the report, such as the positional
        processing rules.
        """
        self.collection_name = settings._NODE_COLLECTION_NAME # name of the MongoDB collection
        self.record = TaxonomyRecord
        # positional processing rules
        self.title_position = None # zero-based position of the record set title
        self.header_position =  0 # zero-based position of the record set Longitude' in record:
        self.data_position = 1 # zero-based position of the record set
        self.num_empty_rows_eod = 0 # data runs until end of file
        self.dialect=CommaDialect()

class TaxonomyRecord(Record):
    """ class that represents the mondoDB taxonomy document """

    @property
    def schema(self):
        """ the cerberus schema definition used for validation of this record """
        return {
            # _id is 'sci_name'
            'taxon_order': { 'type': 'number', 'nullable': True},
            'primary_com_name': { 'type': 'string', 'required': True},
            'category':{ 'type': 'string', 'nullable': True},
            'order_name': { 'type': 'string', 'nullable': True},
            'family_name': { 'type': 'string', 'nullable': True},
            'subfamily_name': { 'type': 'string', 'nullable': True},
            'genus_name': { 'type': 'string', 'nullable': True},
            'species_name': { 'type': 'string', 'nullable': True}}

    def __init__(self, header_row, provider_map, collection_name, row_count, mongo_connection):
        super(TaxonomyRecord, self).__init__()
        self.header_row = header_row
        self.provider_map = provider_map
        if provider_map == None:
            raise InvalidRecordProperty('Record "provider_map" property is None')
        self.provider_map_keys_lower = map(lambda x: x.lower(), provider_map.keys())
        self.collection_name = collection_name
        self.row_count = row_count
        self.validator = Validator(self.schema)

    def create(self, row):
        """ populate the fields with the row data

            The self.fields property will be populated with the column data. An
            ordered dictionary is used as insertion order is critical to
            maintaining positioning with the header.  The order of the headers
            within the file is irrelevant but the data must match.

            Parameters
            ----------
                row : object
                    The parsed row containing column data

            Raises
            ------
                InvalidRecordProperty
                    If the record is missing headers or the headers property
                    is None
                InvalidRecordLength
                    If the record length does not equal the header.
        """
        if not 'header_row' in self.__dict__:
            raise InvalidRecordProperty('Record is missing "header_row" property')
        if self.header_row == None:
            raise InvalidRecordProperty('Record "header_row" property is None')

        header_len = len(self.header_row)
        field_len = len(row)
        if header_len != field_len:
            raise InvalidRecordLength('Record length does not equal header_row')

        position = 0
        for field in row:
            header = self.map_header(self.header_row[position])
            position += 1

            # we ignore none header
            if header == None:
                continue

            # we ignore empty header
            if Record.is_empty_str(header):
                continue

            # special case for unique id
            if header.lower() == 'sci_name':
                if not Record.is_empty_str(field):
                    self.id = field.lower()
                continue

            # all other cases set data-type based on schema
            self.set_field_by_schema(header, field)
