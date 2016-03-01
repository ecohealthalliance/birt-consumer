#!/usr/bin/env python
import os
import sys
import glob
import logging

from conf import settings
from tools.birt_consumer import BirtConsumer

""" wrapper for running BirtConsumer """
if __name__ == '__main__':
    cmd = BirtConsumer()
    cmd.run()
