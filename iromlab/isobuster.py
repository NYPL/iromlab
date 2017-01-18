#! /usr/bin/env python

import os
import config
import shared
from isolyzer import isolyzer

# Wrapper module for IsoBuster

def extractData(writeDirectory, session):
    # IsoBuster /d:i /ei:"E:\nimbieTest\myDiskIB.iso" /et:u /ep:oea /ep:npc /c /m /nosplash /s:1 /l:"E:\nimbieTest\ib.log"
    
    isoFile = os.path.join(writeDirectory, "disc.iso")
    logFile = ''.join([config.tempDir,shared.randomString(12),".log"])

    args = [config.isoBusterExe]
    args.append("".join(["/d:", config.cdDriveLetter, ":"]))
    args.append("".join(["/ei:", isoFile]))
    args.append("/et:u")
    args.append("/ep:oea")
    args.append("/ep:npc")
    args.append("/c")
    args.append("/m")
    args.append("/nosplash")
    args.append("".join(["/s:",str(session)]))
    args.append("".join(["/l:", logFile]))

    # Command line as string (used for logging purposes only)
    cmdStr = " ".join(args)

    status, out, err = shared.launchSubProcess(args)

    fLog = open(logFile, 'r')
    log = fLog.read()

    fLog.close()
    os.remove(logFile)
    
    # Run isolyzer ISO
    try:
        isolyzerResult = isolyzer.processImage(isoFile)
        
        # Isolyzer status
        isolyzerSuccess = isolyzerResult.find('statusInfo/success').text
        
        # Is ISO image smaller than expected (if True, this indicates the image may be truncated)
        imageTruncated = isolyzerResult.find('tests/smallerThanExpected').text
                
        # Volume identifier from ISO's Primary Volume Descriptor 
        volumeIdentifier = isolyzerResult.findtext('properties/primaryVolumeDescriptor/volumeIdentifier')
    except IOError:
        volumeIdentifier = ''
        isolyzerSuccess = False
        imageTruncated = True
        

    # All results to dictionary
    dictOut = {}
    dictOut["cmdStr"] = cmdStr
    dictOut["status"] = status
    dictOut["stdout"] = out
    dictOut["stderr"] = err
    dictOut["log"] = log
    dictOut["volumeIdentifier"] = volumeIdentifier
    dictOut["isolyzerSuccess"] = isolyzerSuccess
    dictOut["imageTruncated"] = imageTruncated
        
    return(dictOut)

def ripAudio(writeDirectory, session):
    # Rip audio to WAV (to be replaced by dBPoweramp wrapper in final version)
    # IsoBuster /d:i /ei:"E:\nimbieTest\" /et:wav /ep:oea /ep:npc /c /m /nosplash /s:1 /t:audio /l:"E:\nimbieTest\ib.log"
    # IMPORTANT: the /t:audio switch requires IsoBuster 3.9 (currently in beta) or above!    
    
    logFile = ''.join([config.tempDir,shared.randomString(12),".log"])

    args = [config.isoBusterExe]
    args.append("".join(["/d:", config.cdDriveLetter, ":"]))
    args.append("".join(["/ei:", writeDirectory]))
    args.append("/et:wav")
    args.append("/ep:oea")
    args.append("/ep:npc")
    args.append("/c")
    args.append("/m")
    args.append("/nosplash")
    args.append("".join(["/s:",str(session)]))
    args.append("/t:audio")
    args.append("".join(["/l:", logFile]))

    # Command line as string (used for logging purposes only)
    cmdStr = " ".join(args)

    status, out, err = shared.launchSubProcess(args)

    fLog = open(logFile, 'r')
    log = fLog.read()

    fLog.close()
    os.remove(logFile)

    # All results to dictionary
    dictOut = {}
    dictOut["cmdStr"] = cmdStr
    dictOut["status"] = status
    dictOut["stdout"] = out
    dictOut["stderr"] = err
    dictOut["log"] = log
        
    return(dictOut)    
