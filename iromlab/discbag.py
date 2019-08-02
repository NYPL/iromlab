#! /usr/bin/env python
"""Wrapper module for Isobuster"""
import sys
sys.path.append("C:\\Users\\mssa-admin\\dev\\iromlab\\iromlab")
import os
import io
import bagit
from . import config
from . import shared


def extractData(writeDirectory, session, dataTrackLSNStart):
    """Extract data to ISO image using specified session number"""

    # Temporary name for ISO file; base name
    try:
        bagit.make_bag(
            config.cdDriveLetter + ":", checksums = ["md5"],
             dest_dir = os.path.join(writeDirectory, 'objects', os.path.basename(writeDirectory) + '_bag')
        )
    except:
        # All results to dictionary
        dictOut = {}
        dictOut["cmdStr"] = 'testcmd'
        dictOut["status"] = 'complete'
        dictOut["stdout"] = 'testout'
        dictOut["stderr"] = 'testerr'
        dictOut["log"] = 'testlog'
        dictOut["volumeIdentifier"] = 'volumeLabel'
        dictOut["bagSuccess"] = False


    # All results to dictionary
    dictOut = {}
    dictOut["cmdStr"] = 'testcmd'
    dictOut["status"] = 'complete'
    dictOut["stdout"] = 'testout'
    dictOut["stderr"] = 'testerr'
    dictOut["log"] = 'testlog'
    dictOut["volumeIdentifier"] = 'volumeLabel'
    dictOut["bagSuccess"] = True

    return dictOut
