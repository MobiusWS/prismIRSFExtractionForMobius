# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import os
import sys

import pandas as pd
import pathlib
import requests
import json

from emails.SendMail import sendEmail
from parsePrismFile import extractZipFile, createMobiusFiles


def extractAndSentIRSFFiles(config):
    # Use a breakpoint in the code line below to debug your script.
    #print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
    #print(argv[1])
    #print(argv[2])
    with open(config, "r") as jsonFile:
        conf = json.load(jsonFile)
        _zipDir = conf['_zipDir']
        _targetDir = conf['_targetDir']
        mailContent = conf['mailContent']
        toEmailAddresses = conf['toEmailAddresses']
        attachments = conf['attachments']

    #extract the donwloaded zipfile
    print(_zipDir)

    prismFile = os.listdir(_zipDir)[0]
    # prismFile=_zipDir+"PRISM_NUMBERS_"
    # extractZipFile(prismFile+_targetDate+".zip", _targetDir)
    extractedCSV = extractZipFile(_zipDir + prismFile, _targetDir)
    filesToAttach= createMobiusFiles(extractedCSV, attachments)
    #toEmailAddress = ['hnagar@gmail.com']
    sendEmail(mailContent, toEmailAddresses, filesToAttach)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    jsonConfigFile = "config/prismMobiusConfig.json"
    extractAndSentIRSFFiles(jsonConfigFile)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
