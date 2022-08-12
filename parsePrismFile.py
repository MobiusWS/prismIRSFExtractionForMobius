from datetime import datetime
import sys
import pandas as pd
import zipfile
import os
import re
def main(argv):

    print(argv[1])
    print(argv[2])



    #extract the donwloaded zipfile
    _zipDir = argv[1]
    _targetDir = argv[2]
    prismFile = os.listdir(_zipDir)[0]
    #prismFile=_zipDir+"PRISM_NUMBERS_"
    #extractZipFile(prismFile+_targetDate+".zip", _targetDir)
    #extractedCSV = extractZipFile(_zipDir+prismFile, _targetDir)
    #createMobiusFiles(extractedCSV)


def createMobiusFiles(extractedCSV, attachments):

    df = pd.read_csv(extractedCSV, skiprows=2)

    prismDate = re.findall(r'\d+', extractedCSV)
    separator = "/"

    df['Latest Update Date'] = pd.to_datetime(df['Latest Update Date'], format="%d/%m/%Y")
    date = datetime.now().strftime("%d_%m_%Y")

#start_date='15/08/2021'
    df = df[df['Latest Update Date'] >= pd.to_datetime(separator.join(prismDate), format="%d/%m/%Y")]
    df = df[['Terminating Country', 'Test Number']]

    compression_opts = dict(method='zip', archive_name=attachments[0]['file'] + '.csv')
    df.to_csv(attachments[0]['file']+"_"+str(date)+".zip", index=False, header=False, compression=compression_opts)

    df = df['Test Number']
    compression_opts = dict(method='zip', archive_name=attachments[1]['file'] + '.csv')
    df.to_csv(attachments[1]['file']+"_"+str(date)+".zip", index=False, header=False, compression=compression_opts)

    filesToAttach=[]
    filesToAttach.append(attachments[0]['file'] + "_" + str(date) + ".zip")
    filesToAttach.append(attachments[1]['file'] + "_" + str(date) + ".zip")
    return filesToAttach

def extractZipFile(_zipFilePath, _targetDir):
    with zipfile.ZipFile(_zipFilePath, 'r') as zip_ref:
        extracted=zip_ref.namelist()
        zip_ref.extractall(_targetDir)
        zip_ref.close()
        extracted_file=os.path.join(_targetDir, extracted[0])
        return extracted_file


if __name__ == "__main__":
    print(sys.argv)
    main(sys.argv)
