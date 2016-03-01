import logging
import os

# Application constants

# debugging
_DEBUG = True

if (_DEBUG):
    logging.getLogger().setLevel(logging.DEBUG)

_DATA_DIR = '/data/'

# command-line options
_ALLOWED_FILE_EXTENSIONS = ['.tsv','.csv']

# types
_TAXONOMY_TYPE = 'Taxonomy'
_MIGRATION_CHECKLIST_TYPE = 'Checklist'
_MIGRATION_CORE_TYPE = 'Core'
_TYPES = [_TAXONOMY_TYPE, _MIGRATION_CHECKLIST_TYPE, _MIGRATION_CORE_TYPE]

# default strftime format for a records date field
_STRFTIME_FORMAT = '%b %Y'

# mongodb collection names
_NODE_COLLECTION_NAME = 'birds'
_PATH_COLLECTION_NAME = 'migrations'
_INVALID_RECORD_COLLECTION_NAME = 'invalidRecords'

# schema
_DISABLE_SCHEMA_MATCH = True #raise exception for headers not in the schema?

# number of lines to split the file, multiprocessing may be faster the
# higher this is set but it will also consume more memory
_CHUNK_SIZE = 5000

# drop indexes?  Setting this to 'true' will drop any existing indexes in the
# database.  However, it is important to remember to then build the indexes
# through birt_ensure_index.py
_DROP_INDEXES = True

# default command-line options
# Allow environment variables for MONGO_HOST, MONGO_DATABASE, MONGO_USERNAME,
# and MONGO_PASSWORD to override these settings
if 'MONGO_HOST' in os.environ:
    _MONGO_HOST = os.environ['MONGO_HOST']
else:
    _MONGO_HOST = 'localhost'

if 'MONGO_DATABASE' in os.environ:
    _MONGO_DATABASE = os.environ['MONGO_DATABASE']
else:
    _MONGO_DATABASE = 'birt'

if 'MONGO_USERNAME' in os.environ:
    _MONGO_USERNAME = os.environ['MONGO_USERNAME']
else:
    _MONGO_USERNAME = None

if 'MONGO_PASSWORD' in os.environ:
    _MONGO_PASSWORD = os.environ['MONGO_PASSWORD']
else:
    #Warning: setting _MONGO_PASSWORD here will be saved as plain-text
    _MONGO_PASSWORD = None
