import logging

from datetime import datetime, timedelta
from cerberus import Validator
from conf import settings

from records.record import InvalidRecord, InvalidRecordLength, InvalidRecordProperty, Record
from tools.csv_helpers import TabDialect, CommaDialect

class MigrationChecklistType(object):
    """ class that represents the .csv format of a birt checklist migration type """

    @property
    def map(self):
        return {
            'sampling_event_id' : { 'maps_to': 'sampling_event_id'},
            'loc_id' : { 'maps_to': 'loc_id'},
            'latitude' : { 'maps_to': 'latitude'},
            'longitude' : { 'maps_to': 'longitude'},
            'year' : { 'maps_to': 'year'},
            'month' : { 'maps_to': 'month'},
            'day' : { 'maps_to': 'day'},
            'time' : { 'maps_to': 'time'},
            'country' : { 'maps_to': 'country'},
            'state_province' : { 'maps_to': 'state_province'},
            'county' : { 'maps_to': 'county'},
            'count_type' : { 'maps_to': 'count_type'},
            'effort_hrs' : { 'maps_to': 'effort_hrs'},
            'effort_distance_km' : { 'maps_to': 'effort_distance_km'},
            'effort_area_ha' : { 'maps_to': 'effort_area_ha'},
            'observer_id' : { 'maps_to': 'observer_id'},
            'number_observers' : { 'maps_to': 'number_observers'},
            'group_id' : { 'maps_to': 'group_id'},
            'primary_checklist_flag' : { 'maps_to': 'primary_checklist_flag'}}

    def __init__(self):
        """ MigrationCoreType constructor

        Describes the 'contract' for the report, such as the positional
        processing rules.
        """
        self.collection_name = settings._PATH_COLLECTION_NAME # name of the MongoDB collection
        self.record = MigrationChecklistRecord
        # positional processing rules
        self.title_position = None # zero-based position of the record set title
        self.header_position = 0 # zero-based position of the record set header
        self.data_position = 1 # zero-based position of the record set
        self.num_empty_rows_eod = 0 # data runs until end of file
        self.dialect=CommaDialect()

class MigrationChecklistRecord(Record):
    """ class that represents the mondoDB migration document """

    @property
    def schema(self):
        """ the cerberus schema definition used for validation of this record """
        return {
            # _id is samplingEventID
            'loc_id' : { 'type': 'string', 'nullable': True, 'required': False},
            'loc': { 'type': 'dict', 'schema': {
                'type': {'type': 'string'},
                'coordinates': {'type': 'list'}}, 'nullable': False},
            'year' : { 'type': 'integer', 'nullable': False, 'required': True},
            'month' : { 'type': 'integer', 'nullable': False, 'required': True},
            'day' : { 'type': 'integer', 'nullable': False, 'required': True},
            'time' : { 'type': 'number', 'nullable': True},
            'country' : { 'type': 'string', 'nullable': True},
            'state_province' : { 'type': 'string', 'nullable': True},
            'county' : { 'type': 'string', 'nullable': True},
            'count_type' : { 'type': 'string', 'nullable': True},
            'effort_hrs' : { 'type': 'number', 'nullable': True},
            'effort_distance_km' : { 'type': 'number', 'nullable': True},
            'effort_area_ha' : { 'type': 'number', 'nullable': True},
            'observer_id' : { 'type': 'string', 'nullable': True},
            'number_observers' : { 'type': 'integer', 'nullable': True},
            'group_id' : { 'type': 'string', 'nullable': True},
            'primary_checklist_flag' : { 'type': 'boolean', 'nullable': True}}

    def __init__(self, header_row, provider_map, collection_name, row_count, mongo_connection):
        """ MigrationChecklistRecord constructor

            Parameters
            ----------
                header_row : list
                    The parsed header row
                collection_name: str
                    The name of the mongoDB collection corresponding to this
                    record
                mongo_connection: object
                    The mongoDB connection
        """
        super(MigrationChecklistRecord, self).__init__()
        self.header_row = header_row
        self.provider_map = provider_map
        self.provider_map_keys_lower = map(lambda x: x.lower(), provider_map.keys())
        self.collection_name = collection_name
        self.row_count = row_count
        self.mongo_connection = mongo_connection
        self.validator = Validator(self.schema, transparent_schema_rules=True, allow_unknown=True)

    @staticmethod
    def is_valid_coordinate_pair(coordinates):
        """ validates that a pair of coordinates are floats representing
        longitudes and latitudes

            Parameters
            ----------
                coordinates: list
                    The coordinate pair as [longitude,latitude]
        """
        longitude = coordinates[0]
        latitude = coordinates[1]

        if longitude == None or latitude == None:
            return False

        if latitude < -90.0 or latitude > 90.0:
            return False

        if longitude < -180.0 or longitude > 180.0:
            return False

        return True

    def gen_date(self):
        """ generate a datetime object from the fields year, month, and day """
        if len(self.fields) == 0:
            return

        if self.validator.validate(self.fields) == False:
            return

        date = None
        try:
            theYear = datetime(self.fields['year'], 1, 1)
            date = theYear + timedelta(days=self.fields['day'] - 1)
        except ValueError as e:
            logging.info('Invalid date: year: %r, day: %r', self.fields['year'], self.fields['day'])
        return date

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

        # default coordinates are null
        coordinates = [None, None]

        position = 0
        for field in row:
            unmappedHeader = self.header_row[position]
            header = self.map_header(unmappedHeader)
            position += 1

            # we create unmappeded schema header as nullable integers to
            # represent the taxonomy counts
            if header == None:
                sanitized = Record.sanitize_key(unmappedHeader)
                if self.could_be_int(field):
                    value = int(field)
                    if value > 0:
                        self.fields[sanitized] = int(field)
                continue

            # we ignore empty headers
            if Record.is_empty_str(header):
                continue

            # special case for unique id
            if header.lower() == 'sampling_event_id':
                if not Record.is_empty_str(field):
                    self.id = field
                continue

            # special cases to convert to geoJSON
            # Always list coordinates in longitude, latitude order.
            if header.lower() == 'longitude':
                if Record.could_be_float(field):
                    coordinates[0] = float(field)
                continue
            if header.lower() == 'latitude':
                if Record.could_be_float(field):
                    coordinates[1] = float(field)
                continue

            # all other cases set data-type based on schema
            self.set_field_by_schema(header, field)

        #we cannot have invalid geoJSON objects in mongoDB
        if MigrationChecklistRecord.is_valid_coordinate_pair(coordinates):
            loc = {
                'type': 'Point',
                'coordinates': coordinates
            }
        else:
            loc = None

        #add the geoJSON 'loc'
        self.fields['loc'] = loc

        #add the generated date
        self.fields['date'] = self.gen_date()

class MigrationCoreType(object):
    """ class that represents the .csv format of a birt core migration type """

    @property
    def map(self):
        return {
            'sampling_event_id' : { 'maps_to': 'sampling_event_id'},
            'loc_id' : { 'maps_to': 'loc_id'},
            'pop00_sqmi' : { 'maps_to': 'pop00_sqmi'},
            'housing_density' : { 'maps_to': 'housing_density'},
            'housing_percent_vacant' : { 'maps_to': 'housing_percent_vacant'},
            'elev_gt' : { 'maps_to': 'elev_gt'},
            'elev_ned' : { 'maps_to': 'elev_ned'},
            'bcr' : { 'maps_to': 'bcr'},
            'bailey_ecoregion' : { 'maps_to': 'bailey_ecoregion'},
            'omernik_l3_ecoregion' : { 'maps_to': 'omernik_l3_ecoregion'},
            'caus_temp_avg' : { 'maps_to': 'caus_temp_avg'},
            'caus_temp_min' : { 'maps_to': 'caus_temp_min'},
            'caus_temp_max' : { 'maps_to': 'caus_temp_max'},
            'caus_prec' : { 'maps_to': 'caus_prec'},
            'caus_snow' : { 'maps_to': 'caus_snow'}}

    def __init__(self):
        """ MigrationCoreType constructor

        Describes the 'contract' for the report, such as the positional
        processing rules.
        """
        self.collection_name = settings._PATH_COLLECTION_NAME # name of the MongoDB collection
        self.record = MigrationCoreRecord
        # positional processing rules
        self.title_position = None # zero-based position of the record set title
        self.header_position = 0 # zero-based position of the record set header
        self.data_position = 1 # zero-based position of the record set
        self.num_empty_rows_eod = 0 # data runs until end of file
        self.dialect=CommaDialect()

class MigrationCoreRecord(Record):
    """ class that represents the mondoDB migration core document """

    @property
    def schema(self):
        """ the cerberus schema definition used for validation of this record """
        return {
            # _id is sampling_event_id
            'loc_id': { 'type': 'string', 'nullable': True},
            'pop00_sqmi': { 'type': 'number', 'nullable': True},
            'housing_density': { 'type': 'number', 'nullable': True},
            'housing_percent_vacant': { 'type': 'number', 'nullable': True},
            'elev_gt': { 'type': 'integer', 'nullable': True},
            'elev_ned': { 'type': 'number', 'nullable': True},
            'bcr': { 'type': 'integer', 'nullable': True},
            'bailey_ecoregion': { 'type': 'string', 'nullable': True},
            'omernik_l3_ecoregion': { 'type': 'integer', 'nullable': True},
            'caus_temp_avg': { 'type': 'integer', 'nullable': True},
            'caus_temp_min': { 'type': 'integer', 'nullable': True},
            'caus_temp_max': { 'type': 'integer', 'nullable': True},
            'caus_prec': { 'type': 'integer', 'nullable': True},
            'caus_snow': { 'type': 'integer', 'nullable': True}}

    def __init__(self, header_row, provider_map, collection_name, row_count, mongo_connection):
        """ MigrationCoreRecord constructor

            Parameters
            ----------
                header_row : list
                    The parsed header row
                collection_name: str
                    The name of the mongoDB collection corresponding to this
                    record
                mongo_connection: object
                    The mongoDB connection
        """
        super(MigrationCoreRecord, self).__init__()
        self.header_row = header_row
        self.provider_map = provider_map
        self.provider_map_keys_lower = map(lambda x: x.lower(), provider_map.keys())
        self.collection_name = collection_name
        self.row_count = row_count
        self.mongo_connection = mongo_connection
        self.validator = Validator(self.schema, transparent_schema_rules=True, allow_unknown=True)

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

        position = 0
        for field in row:
            unmappedHeader = self.header_row[position]
            header = self.map_header(unmappedHeader)
            position += 1

            # we create unmappeded schema header as nullable numbers to
            # represent the NLCD* headers
            if header == None:
                sanitized = Record.sanitize_key(unmappedHeader)
                if self.could_be_number(field):
                    self.fields[sanitized] = float(field)
                else:
                    self.fields[sanitized] = None
                continue

            # we ignore empty headers
            if Record.is_empty_str(header):
                continue

            # special case for unique id
            if header.lower() == 'sampling_event_id':
                if not Record.is_empty_str(field):
                    self.id = field;
                continue

            # all other cases set data-type based on schema
            self.set_field_by_schema(header, field)
