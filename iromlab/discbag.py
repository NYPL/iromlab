#! /usr/bin/env python
"""Wrapper module for Isobuster"""
import sys
sys.path.append("C:\\Users\\mssa-admin\\dev\\iromlab\\iromlab")
import os
import io
import shutil
import bagit
from . import config
from . import shared


def extractData(writeDirectory):
    """Extract data to ISO image using specified session number"""

    bag_dir = os.path.join(writeDirectory, 'objects', os.path.basename(writeDirectory) + '_bag')

    imagedInfo = {'byte_count': 0, 'file_count' = 0}
    try:
        bag = bagit.make_bag(
            config.cdDriveLetter + ":", checksums = ["md5"], dest_dir = bag_dir
        )
        imaged_info['byte_count'], imaged_info['file_count'] = bag.info.get("Payload-Oxum").split(".", 1)
        success = True
        reject = False
    except:
        shutil.rmtree(os.path.join(writeDirectory, 'objects'))
        byte_count, file_count = 0, 0
        success = False
        reject = True

    return success, reject, imagedInfo
