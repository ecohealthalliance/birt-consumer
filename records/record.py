import json
import collections
import logging

from datetime import datetime
from cerberus import Validator
from bson import json_util
from conf import settings

class InvalidRecordProperty(Exception):
    """ custom exception that is thrown when the record is missing required
    properties """
    def __init__(self, message, *args, **kwargs):
        """ InvalidRecordProperty constructor

            Parameters
            ----------
                message : str
                    A descriptive message of the error
        """
        super(InvalidRecordProperty, self).__init__(message)

class InvalidRecordLength(Exception):
    """ custom exception that is thrown when the data to be inserted into the
    record does not match the header length """
    def __init__(self, message, *args, **kwargs):
        """ InvalidRecordLength constructor

            Parameters
            ----------
                message : str
                    A descriptive message of the error
        """
        super(InvalidRecordLength, self).__init__(message)

class InvalidRecord(object):
    """ class that represents the mondoDB format of an invalid record.  This
    is created when the file reader parses an invalid row."""

    @property
    def schema(self):
        """ the cerberus schema defination used for validation of a record """
        return {
            'Date': { 'type': 'datetime', 'required': True},
            'Errors': { 'type': 'dict', 'required': True},
            'RecordType': {'type': 'string', 'required': True},
            'RowNum': { 'type': 'integer', 'nullable': True}
        }

    def __init__(self, errors, record_type, row_num):
        """ InvalidRecord constructor

            Parameters
            ----------
                errors: list
                    List of validation errors with the last row containing
                    the records fields
                record_type: object
                    The type of record {AirportType, FlightType}
                row_num: int
                    The row number that the validation error occurred
        """
        self.fields = collections.OrderedDict()
        self.fields['Date'] = datetime.utcnow()
        self.fields['Errors'] = errors
        self.fields['RecordType'] = record_type
        self.fields['RowNum'] = row_num
        self.validator = Validator(self.schema)

    def validate(self):
        """ validates the record against the schema """
        return self.validator.validate(self.fields)

    def to_json(self):
        """ dumps the records fields into JSON format """
        return json.dumps(self.fields)

class Record(object):
    """ base record class

        A record is an object that contains fields (ordered dictionary) that
        are used to construct a mongoDB document.
    """

    @property
    def id(self):
        return self._id;

    @id.setter
    def id(self, val):
        self._id = val

    def __init__(self):
        """ Record constructor """
        self.fields = collections.OrderedDict()
        self.row_count = None
        self._id = None

    @staticmethod
    def is_empty_str(val):
        """ check if the val is an empty string"""
        s = str(val)
        if not isinstance(s, str):
            return False
        if not s.strip():
            return True
        else:
            return False

    @staticmethod
    def could_be_float(val):
        """ determines if the val is an instance of float or could be coerced
        to a float from a string """
        if val == None:
            return False

        if isinstance(val, float):
            return True

        # allow coercion from str
        if isinstance(val, (str, unicode)):
            try:
                f = float(val)
                if not isinstance(f, float):
                    raise ValueError
                else:
                    return True
            except:
                return False

        #otherwise
        return False

    @staticmethod
    def could_be_int(val):
        """ determines if the val is an instance of int or could be coerced
        to an int from a string """
        if val == None:
            return False

        if isinstance(val, int):
            return True

        # allow coercion from str
        if isinstance(val, (str, unicode)):
            try:
                i = int(val)
                if not isinstance(i, int):
                    raise ValueError
                else:
                    return True
            except:
                return False

        # otherwise
        return False

    @staticmethod
    def could_be_number(val):
        """ determines if the val is an instance of 'number' or could be coerced
        to a 'number' from a string """
        if val == None:
            return False

        if isinstance(val, (float, int, long)):
            return True

        # allow coercion from str
        if isinstance(val, (str, unicode)):
            try:
                n = float(val)
                if not isinstance(n, float):
                    raise ValueError
                else:
                    return True
            except:
                return False

        #otherwise
        return False

    @staticmethod
    def could_be_boolean(val):
        """ determines if the val is an instance of bool or could be coerced
        to a bool from a string or int, long"""
        if val == None:
            return False

        if isinstance(val, bool):
            return True

        if isinstance(val, (str, unicode)):
            if val.lower() in ['true', '1', 'false', '0']:
                return True

        if isinstance(val, (int, long)):
            if val in [0,1]:
                return True

        return False

    @staticmethod
    def could_be_datetime(val, fmt):
        """ determines if the val is an instance of datetime or could be coerced
        to a datetime from a string with the provided format"""

        if val == None or fmt == None:
            return False

        if isinstance(val, datetime):
            return True

        if isinstance(val, (str, unicode)):
            if Record.is_empty_str(val) or Record.is_empty_str(fmt):
                return False

            try:
                d = datetime.strptime(val, fmt)
                if not isinstance(d, datetime):
                    raise ValueError
                else:
                    return True
            except Exception as e:
                logging.error(e)
                return False

        #otherwise
        return False

    @staticmethod
    def parse_boolean(field):
        if Record.could_be_boolean(field):
            if Record.could_be_int(field):
                return bool(int(field))

            if isinstance(field, (str, unicode)):
                if field.lower() == 'true':
                    return True
                if field.lower() == 'false':
                    return False

            return bool(field)
        return None

    def set_field_by_schema(self, header, field):
        """ allows the records field to be set by matching against the schema

            NOTE: InvalidRecordProperty is raised if the header isn't located
            within the schema.  This check can be disabled through the
            constant '_DISABLE_SCHEMA_MATCH' in settings.py.
            The side-effect would be documents in the mongoDB would have
            different structure but added flexibility to the parser

            Parameters
            ----------
            header: str
                the name of the header
            field: str
                the corresponding column field of the row

            Raises
            ------
            InvalidRecordProperty
                If the header value is not located within the schema
        """
        if header not in self.schema.keys():
            if settings._DISABLE_SCHEMA_MATCH:
                return
            else:
                raise InvalidRecordProperty('Record schema does not have the property "%s"' % header)

        data_type = self.schema[header]['type'].lower()

        if data_type == 'string':
            if Record.is_empty_str(field):
                self.fields[header] = None
            elif field == '?':
                self.fields[header] = None
            else:
                self.fields[header] = field
            return

        if data_type == 'integer':
            if Record.could_be_int(field):
                self.fields[header] = int(field)
            else:
                self.fields[header] = None
            return

        if data_type == 'datetime':
            datetime_format = self.schema[header]['datetime_format'];
            if datetime_format == None:
                datetime_format = settings._STRFTIME_FORMAT
            if Record.could_be_datetime(field, datetime_format):
                self.fields[header] = datetime.strptime(field, datetime_format)
            else:
                self.fields[header] = None
            return

        if data_type == 'number':
            if Record.could_be_number(field):
                self.fields[header] = float(field)
            else:
                self.fields[header] = None
            return

        if data_type == 'float':
            if Record.could_be_float(field):
                self.fields[header] = float(field)
            else:
                self.fields[header] = None
            return

        if data_type == 'boolean':
            self.fields[header] = Record.parse_boolean(field)
            return

    def validation_errors(self):
        errors = self.validator.errors
        if len(errors.keys()) > 0:
            errors['fields'] = self.to_json()
        return errors

    def validate(self):
        """ validate the record

            This is a combination of checking the property _id is not None
            and that all fields within the schema are valid
        """
        if self.id == None:
            return False
        return self.validator.validate(self.fields)

    def map_header(self, header):
        if header.lower() in self.provider_map_keys_lower:
            return self.provider_map[header.lower()]['maps_to']
        return None

    def to_json(self):
        return json.dumps(self.fields, default=json_util.default)
