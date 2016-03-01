#!/usr/bin/env python
import os
import sys
import glob

from conf import settings
from tools.birt_ensure_indexes import BirtEnsureIndexes


""" wrapper for running BirtEnsureIndexes """
if __name__ == '__main__':

    cmd = BirtEnsureIndexes()
    cmd.run()
