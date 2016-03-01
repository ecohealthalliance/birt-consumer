# birt-consume

A Python script to parse The eBird Reference Dataset, Version 2013 into MongoDB
with geospatial indexing.

## Install

1. setup virtualenv

  ``` virtualenv env```

2. activate the virtual environment

  ``` source env/bin/activate```

3. install 3rd party libraries

  ``` pip install -r requirements.txt```

## User Defined Settings

### Environment variables
Environment variables may be used for [MONGO_HOST, MONGO_DATABASE, MONGO_USERNAME, MONGO_PASSWORD].
Note:  these will override any value set within settings.py but not arguments to the program

Ex: `~/git/birt-consume$ MONGO_HOST='10.0.1.2' python birt_consume.py`

### settings.py

User defined settings may be set within `conf/settings.py`, which include:

  ```
  _DEBUG #boolean, true enables logging.debug messages
  _DATA_DIR #string, location of the FTP downloaded files ex '/data/'
  _ALLOWED_FILE_EXTENSIONS #array, allowed extensions for data files ex. ['.tsv','.csv']
  _TYPES = #array, types of data files ex. ['Taxonomy', 'Checklist', 'Core']
  _STRFTIME_FORMAT #string, default strftime format for a records date field ex. '%b %Y'
  _NODE_COLLECTION_NAME #string, mongodb collection names ex. 'birds'
  _PATH_COLLECTION_NAME #string, mongodb collection names ex. 'migrations'
  _INVALID_RECORD_COLLECTION_NAME #string, mongodb collection names ex. 'invalidRecords'
  _DISABLE_SCHEMA_MATCH #boolean, raise exception for headers not in the schema?
  _CHUNK_SIZE #integer, number of lines to split the input file
  _MONGO_HOST #string, default command-line option for when -m is not specified ex. 'localhost'
  _MONGO_DATABASE #string, default command-line option for when -d is not specified ex. 'birt'
  _MONGO_USERNAME #string or None, default command-line option for when -u is not specified ex. None
  # Warning: this will be stored in plain-text
  _MONGO_PASSWORD #string or None, default command-line option for when -p is not specified ex. None
  ```

## Run

1. Upsert taxonomy data (NOTE: This would be done a periodic basis, such as once
   a month.  However, it must be done at least once prior to running the
   'Checklist' and 'Core' data imports below.)

  ```
  python birt_consume.py --type Taxonomy tests/data/taxonomy.csv
  ```

2. Upsert 'Checklist' and 'Core' migration reports:

  ```
  python birt_consume.py --type Checklist tests/data/checklists.csv
  python birt_consume.py --type Core tests/data/core.csv
  ```

3. Create the indexes on the database
  ```
  python birt_ensure_index.py
  ```

## Program Options

  ```
    usage: birt_consume.py [-h] [-v] -t {Taxonomy,Checklist,Core} [-u USERNAME]
                           [-p PASSWORD] [-d DATABASE] [-m MONGOHOST]
                           infile

    script to parse bird migration data file and populate a mongodb collection.

    positional arguments:
      infile                the file to be parsed

    optional arguments:
      -h, --help            show this help message and exit
      -v, --verbose         verbose output
      -t {Taxonomy,Checklist,Core}, --type {Taxonomy,Checklist,Core}
                            the type of provider to be parsed
      -u USERNAME, --username USERNAME
                            the username for mongoDB (Default: None)
      -p PASSWORD, --password PASSWORD
                            the password for mongoDB (Default: None)
      -d DATABASE, --database DATABASE
                            the database for mongoDB (Default: birt)
      -m MONGOHOST, --mongohost MONGOHOST
                            the hostname for mongoDB (Default: localhost)
  ```

  ```
    usage: birt_ensure_index.py [-h] [-u USERNAME] [-p PASSWORD] [-d DATABASE]
                                [-m MONGOHOST]

    script to set the mongodb indexes for birt taxonomy and migration data.

    optional arguments:
      -h, --help            show this help message and exit
      -u USERNAME, --username USERNAME
                            the username for mongoDB (Default: None)
      -p PASSWORD, --password PASSWORD
                            the password for mongoDB (Default: None)
      -d DATABASE, --database DATABASE
                            the database for mongoDB (Default: birt)
      -m MONGOHOST, --mongohost MONGOHOST
                            the hostname for mongoDB (Default: localhost)
  ```


## License
Copyright 2016 EcoHealth Alliance

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
