import os
import sys
import time
import argparse
import logging

from tools.birt_mongo import BirtMongoConnection
from conf import settings

class BirtEnsureIndexes(object):
    """ Command line tool to set indexes within mongodb """

    def __init__(self):
        self.parser = argparse.ArgumentParser(description='script to set ' \
            'the mongodb indexes for birt taxonomy and migration data.')

    def add_args(self):
        """ add arguments to the argparse command-line program """
        self.parser.add_argument('-u', '--username',
            default=None,
            help='the username for mongoDB (Default: None)')

        self.parser.add_argument('-p', '--password',
            default=None,
            help='the password for mongoDB (Default: None)')

        self.parser.add_argument('-d', '--database',
            default='birt',
            help='the database for mongoDB (Default: birt)')

        self.parser.add_argument('-m', '--mongohost',
            default='localhost',
            help='the hostname for mongoDB (Default: localhost)')

    def query_yes_no(self, question, default="yes"):
        """
        http://code.activestate.com/recipes/577058/
        Ask a yes/no question via raw_input() and return their answer.

        "question" is a string that is presented to the user.
        "default" is the presumed answer if the user just hits <Enter>.
            It must be "yes" (the default), "no" or None (meaning
            an answer is required of the user).

        The "answer" return value is True for "yes" or False for "no".
        """
        valid = {"yes": True, "y": True, "ye": True,
                 "no": False, "n": False}
        if default is None:
            prompt = " [y/n] "
        elif default == "yes":
            prompt = " [Y/n] "
        elif default == "no":
            prompt = " [y/N] "
        else:
            raise ValueError("invalid default answer: '%s'" % default)

        while True:
            logging.warn(question + prompt)
            choice = raw_input().lower()
            if default is not None and choice == '':
                return valid[default]
            elif choice in valid:
                return valid[choice]
            else:
                logging.warn("Please respond with 'yes' or 'no' "
                                 "(or 'y' or 'n').\n")

    def run(self, *args):
        """ kickoff the program """
        self.add_args()

        if len(args) > 0:
            program_args = self.parser.parse_args(args)
        else:
            program_args = self.parser.parse_args()

        # setup the mongoDB connection
        mongo_connection = BirtMongoConnection(program_args)

        # Confirm the user wants to apply the indexes
        confirm = self.query_yes_no("This will lock the database.  Are your sure?", "no")
        if confirm:
            # ensure that the indexes are applied to the collections
            result = mongo_connection.ensure_indexes()
            logging.info(result)
