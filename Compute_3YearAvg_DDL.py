'''
Author: Johan Beekhuizen
E-mail: johan.beekhuizen@deltares.nl
Date: 10-06-2016

Compute3YrAvg_DDL
This script computer the average measurement values for measurements retrieved from the Data Distributielaag (DDL)
from Rijkswaterstaat.

All results are stored in a MongoDB database. Therefore, a MongoDB service (mongod.exe) should be running
in order to write the results to the database.
Optionally, the downloaded JSON data from de DDL are stored for reference in a directory set in the INPUT PARAMETERS
on top of this script.

The calculation method for determining the 3-yr average is obtained from the RIVM normendatase:
https://acceptatie.rvs.rivm.nl/Data/SubtanceNormValues

'''

__author__ = 'beekhuiz'

import os, sys
import pymongo
import requests
from datetime import datetime
from dateutil import parser
import ogr, osr
import json
import logging
from logging.handlers import RotatingFileHandler
import numpy as np


#------------------------------------------------------------------#
#-------- INPUT PARAMETERS ----------------------------------------#
#------------------------------------------------------------------#

endtimeDT = datetime(2015,1,1,0,0,0)    # compute for 2012, 2013 and 2014
starttimeDT = datetime(2012,1,1,0,0,0)  # endtimeDT - timedelta(days=365 * 3)

MONGO_DB_CLIENT = "EI_Toets"
MONGO_DB_COLLECTION = "EIData_Test"          # name of the collection in the MongoDB database to which data is written
overwriteExistingCollection = False     # if true, the collection with name MONGO_DB_COLLECTION will be dropped;
                                        # if false, new records ('documents') are added to the current collection

outputEPSG = 4326  # WGS84

# store json files returned by DDL OnlineWaarnemingenService for reference; make sure directory does not exist
storeDDLFiles = False
dataDir = os.path.join("d:/EIToetsOutput", "DDL_DataAllNorms12")    # only relevant if storeDDLFiles = True

logFile = 'd:/EIToetsOutput/ComputeAvgLogAllNorms12.log'

# set max limit nr of records to read from DDL
nrRecords = 20

RIVMNormDBUrl = "https://rvs.rivm.nl/zoeksysteem/Data/SubtanceNormValues"
RWS_Metadata_URL = "https://acceptatie.waterwebservices.rijkswaterstaat.nl/METADATASERVICES_DBO/OphalenCatalogus/"
RWS_Waarnemingen_URL = "https://acceptatie.waterwebservices.rijkswaterstaat.nl/ONLINEWAARNEMINGENSERVICES_DBO/OphalenWaarnemingen/"


#-------------------------------------------------------------------#
#-------- FUNCTIONS ------------------------------------------------#
#-------------------------------------------------------------------#

def transformPoint(x, y, inputEPSG, outputEPSG):
    """
    :param the x and y coordinates of a certain point in the inputEPSG (EPSG code) coordinate system
    the coordinates are transformed to the output EPSG code
    :return: a tuple with the x and y coordinates
    """

    # create a geometry from coordinates
    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(x, y)

    # create coordinate transformation
    inSpatialRef = osr.SpatialReference()
    inSpatialRef.ImportFromEPSG(inputEPSG)

    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(outputEPSG)

    coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

    point.Transform(coordTransform)    # transform point

    pointOutputEPSG = (point.GetX(), point.GetY())

    return pointOutputEPSG


#------------------------------------------------------------------#
#-------- START SCRIPT --------------------------------------------#
#------------------------------------------------------------------#

# connect to database
client = pymongo.MongoClient(serverSelectionTimeoutMS=1)
db = client[MONGO_DB_CLIENT]
#db = client.EI_Toets
if overwriteExistingCollection:     # if the collection is not dropped, new data is added to current collection
    db.drop_collection(MONGO_DB_COLLECTION)
collection = db[MONGO_DB_COLLECTION]

# test if connection to MongoDB works
try:
    client.server_info()
except pymongo.errors.ServerSelectionTimeoutError as err:
    print(err)
    print "Error in connecting or creating MongoDB collection; have you started MongoDB?"
    sys.exit()



#region set up logging
logger = logging.getLogger("data_aansluitpunt_log")
file_handler = RotatingFileHandler(logFile, 'a', 10 * 1024 * 1024, 10)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.setLevel(logging.INFO)
file_handler.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.info('Start computing average measurement values for DDL')
#endregion

starttimeDDString = starttimeDT.strftime("%Y-%m-%dT%H:%M:%S")
endtimeDDString = endtimeDT.strftime("%Y-%m-%dT%H:%M:%S")



#read existing data in database and store parameter + loc codes as unique identifier
mongocursor = collection.find()
loadedTimeSeries = []
for record in mongocursor:
    parCode = record['properties']['aquoParCode']
    locID = record['properties']['locID']
    loadedTimeSeries.append(parCode + "_" + locID)


# create data directory to store downloaded files from DDL
if storeDDLFiles and os.path.exists(dataDir) == False:
    os.makedirs(dataDir)

inputEPSG = 25831   # the DDL uses EPSG 25831


#region Read data from RIVM normendatabase to check if it's P90 or JGM
RIVMString = requests.get(RIVMNormDBUrl)
#RIVMString = requests.get('https://acceptatie.rvs.rivm.nl/Data/SubtanceNormValues', auth=HTTPBasicAuth('rvs', 'nitr@@t'))  # old db
RIVMDict = json.loads(RIVMString.text)  # convert json string to python dict
normsList = RIVMDict['norms']
substancesList = RIVMDict['substances']
logger.info('Normendatabase RIVM loaded')
print "Normendatabase RIVM loaded"
#endregion

#region Get metadata from RWS metadata service
payload = {"CatalogusFilter": {"Grootheden": True, "Parameters": True, "Eenheden": True}}
headers = {'content-type': 'application/json'}
r = requests.post(RWS_Metadata_URL, data=json.dumps(payload), headers=headers)
resultJSON = r.json()


comb = resultJSON['AquoMetadataLocatieLijst']
locations = resultJSON['LocatieLijst']
aquometadatalijst = resultJSON['AquoMetadataLijst']
logger.info('Metadata from DDL loaded')
print "Metadata from DDL loaded"
#endregion


#region TEMP COMPUTE NR TIMESERIES

nrTotalRecords = 0
nrConcRecords = 0
nrRecordsToCompute = 0
nrRecordsAlreadyDone = 0

for record in comb:

    messageID = record['AquoMetaData_MessageID']
    locMessageID = record['Locatie_MessageID']

    aquometadata = [aquometadata for aquometadata in aquometadatalijst if aquometadata.get('AquoMetadata_MessageID') == messageID]
    location = [location for location in locations if location.get('Locatie_MessageID') == locMessageID]

    # stored as list, create dict
    location = location[0]
    aquometadata = aquometadata[0]

    aquoCodeGrootheid = aquometadata['Grootheid']['Code']
    parCode = aquometadata['Parameter']['Code']
    #print parCode

    locID = location['Code']

    if aquoCodeGrootheid == 'CONCTTE':  # only compute avg for grootheid concentratie
        nrConcRecords += 1

        #check if parameter / location combination has already been computed
        uniqComb = parCode + "_" + locID
        if uniqComb in loadedTimeSeries:
            nrRecordsAlreadyDone += 1
        else:
            nrRecordsToCompute += 1

    nrTotalRecords += 1

print "Total concentration records: " + str(nrConcRecords)
print "Records already computed: " + str(nrRecordsAlreadyDone)
print "Records still to compute: " + str(nrRecordsToCompute)
#endregion


n = 0   # keep track of number computed records

for record in comb:

    if n >= nrRecords:
        break

    #region Get location and parameter info from metadata file
    messageID = record['AquoMetaData_MessageID']
    locMessageID = record['Locatie_MessageID']

    location = [location for location in locations if location.get('Locatie_MessageID') == locMessageID]
    aquometadata = [aquometadata for aquometadata in aquometadatalijst if aquometadata.get('AquoMetadata_MessageID') == messageID]

    # stored as list, create dict
    location = location[0]
    aquometadata = aquometadata[0]

    # convert datetime to format used by DDL
    starttimeDDL = starttimeDT.strftime("%Y-%m-%dT%H:%M:%S.000+01:00")
    endtimeDDL = endtimeDT.strftime("%Y-%m-%dT%H:%M:%S.000+01:00")

    parCode = aquometadata['Parameter']['Code']
    locID = location['Code']
    aquoCodeGrootheid = aquometadata['Grootheid']['Code']   # RWS defines for each parameter a 'Grootheid'
    uniqComb = parCode + "_" + locID    # used for checking if this time series has already been processed to the MongoDB

    fileName = os.path.join(dataDir, 'record' + parCode + "_" + str(locMessageID) + '.json')

    logger.info("Compute " + parCode + " and location " + locID)
    print "Compute " + parCode + " and location " + locID
    #endregion


    if aquoCodeGrootheid == 'CONCTTE' and uniqComb not in loadedTimeSeries:  # only compute avg for grootheid concentratie

        # check if parameter / location combination has already been computed
        # uniqComb = parCode + "_" + locID
        # if uniqComb in loadedTimeSeries:
        #     break

        #region Get the data from the OnlineWaarnemingenService
        aquoMetadataType = "Parameter"

        payload = {"AquoPlusWaarnemingMetadata":
                       {"AquoMetadata": {aquoMetadataType: {"Code": parCode}}},
                   "Locatie": {"X": repr(location['X']), "Y": repr(location['Y']), "Code": locID},
                   "Periode": {"Begindatumtijd": starttimeDDL, "Einddatumtijd": endtimeDDL}}

        headers = {'content-type': 'application/json'}

        r = requests.post(RWS_Waarnemingen_URL, data=json.dumps(payload), headers=headers)

        # check if data was retrieved successfully from the DDL
        requestSucces = False

        if r.status_code == 200:
            resultJSON = r.json()
            if 'Succesvol' in resultJSON.keys():
                if resultJSON['Succesvol'] == True:
                    requestSucces = True
                else:
                    logger.error("Key 'Succesvol' is False")
                    print "Key 'Succesvol' is False"
            else:
                logger.error("No key 'Succesvol' in source DDL")
                print "No key 'Succesvol' in source DDL"
        else:
            logger.error("Error in retrieving data from DDL for " + parCode + " and location " + locID + ", status code: " + str(r.status_code))
        # endregion


        if requestSucces:

            # store data of DDL
            if storeDDLFiles:
                if os.path.exists(fileName):
                    logger.error("File " + fileName + " already exists, overwrite this file")
                fo = open(fileName, 'w')
                fo.write(r.content)
                fo.close()

            for waarnemingLijst in resultJSON['WaarnemingenLijst']:

                EIData = {}

                #region store all relevant EI metadata
                aquoMetadata = waarnemingLijst['AquoMetadata']

                EIData['bemonsteringsSoortOmschrijving'] = aquoMetadata['BemonsteringsSoort']['Omschrijving']
                EIData['bemonsteringsSoortCode'] = aquoMetadata['BemonsteringsSoort']['Code']
                EIData['compartimentOmschrijving'] = aquoMetadata['Compartiment']['Omschrijving']
                EIData['compartimentCode'] = aquoMetadata['Compartiment']['Code']
                EIData['hoedanigheidOmschrijving'] = aquoMetadata['Hoedanigheid']['Omschrijving']
                EIData['hoedanigheidCode'] = aquoMetadata['Hoedanigheid']['Code']
                EIData['eenheidOmschrijving'] = aquoMetadata['Eenheid']['Omschrijving']
                EIData['eenheidCode'] = aquoMetadata['Eenheid']['Code']
                EIData['fileName'] = os.path.join(dataDir, fileName)
                #endregion

                #region Get calculation method from RIVM normendatabase

                normIDList = []
                valueProcessingMethodCode = ''

                # find the norm id of this substance by aquocode
                for substance in substancesList:
                    if substance['aquoCode'] == parCode:
                        norms = substance['norms']

                        for norm in norms:
                            normIDList.append(norm['id'])


                # get the processingmethodcodes based on the retrieved id
                normsForSubstanceList = []

                if normIDList:
                    for normID in normIDList:
                        for norm in normsList:
                            if norm['id'] == normID:
                                normInfoDict = {}
                                normInfoDict['valueProcessingMethodCode'] = norm['valueProcessingMethodCode']
                                normInfoDict['valueProcessingMethodDescription'] = norm['valueProcessingMethodDescription']
                                normInfoDict['id'] = norm['id']
                                normInfoDict['stateCode'] = norm['stateCode']
                                normInfoDict['stateDescription'] = norm['stateDescription']
                                normInfoDict['normDescription'] = norm['normDescription']
                                normsForSubstanceList.append(normInfoDict)


                # Get all the norms that have the same Norm StateCode as the metadata Hoedanigheidcode of the DDL-measurements
                normsForSubstanceStateCodeList = []
                valueProcessingMethodCodeList = []

                # match the norm stateCode with the Hoedanigheidscode
                for normInfo in normsForSubstanceList:
                    if normInfo['stateCode'] == EIData['hoedanigheidCode']:
                        normsForSubstanceStateCodeList.append(normInfo)
                        valueProcessingMethodCodeList.append(normInfo['valueProcessingMethodCode'])

                EIData['normsForSubstanceStateCodeList'] = normsForSubstanceStateCodeList
                EIData['valueProcessingMethodCode'] = valueProcessingMethodCodeList

                if "JGM" in valueProcessingMethodCodeList:
                    EIData['valueProcessingMethodCode'] = "JGM"
                elif "MAX" in valueProcessingMethodCodeList:
                    EIData['valueProcessingMethodCode'] = "MAX"
                elif "P90" in valueProcessingMethodCodeList:
                    EIData['valueProcessingMethodCode'] = "P90"
                else:
                    EIData['valueProcessingMethodCode'] = "Other"

                #endregion


                #region Calculate the average

                metingen = waarnemingLijst['MetingenLijst']

                # only compute average if the valueprocessingmethod is JGM, MAX or P90
                if EIData['valueProcessingMethodCode'] in ['JGM', 'MAX', 'P90']:

                    # get all the years
                    years = []
                    meettijden = []

                    for meting in metingen:
                        tijdstip = dt = parser.parse(meting['Tijdstip'])
                        meettijden.append(tijdstip)
                        years.append(tijdstip.year)
                        meting['year'] = tijdstip.year     # add year to the metingen

                    uniqYears = set(years)

                    EIYearData = []
                    avgYears = []   # list of all average values for each year

                    # compute average for each year
                    for year in uniqYears:

                        meetwaarden = []
                        meettijden = []
                        nrInvalidMeas = 0
                        nrValidMeas = 0
                        totalMeas = 0
                        measLimit = -9999

                        for meting in metingen:

                            if meting['year'] == year:

                                metadata = meting['WaarnemingMetadata']

                                #print "Referentievlak: " + metadata['ReferentievlakLijst'][0] + ", kwaliteitscode " + metadata['KwaliteitswaardecodeLijst'][0]
                                if metadata['ReferentievlakLijst'][0] != "WATSGL":
                                    print "Referentievlak: " + metadata['ReferentievlakLijst'][0]

                                if int(metadata['KwaliteitswaardecodeLijst'][0]) < 50 and metadata['ReferentievlakLijst'][0] == "WATSGL":
                                    meetwaarde = meting['Meetwaarde']['Waarde_Numeriek']

                                    #print "ref vlak OK!"

                                    # check if "Waarde Limietsymbool" is one of the keys; if yes, the value is the lower limit
                                    if "Waarde_Limietsymbool" in meting['Meetwaarde'].keys():
                                        measLimit = meetwaarde
                                        meetwaarde *= 0.5

                                    meetwaarden.append(meetwaarde)
                                    meettijden.append(meting['Tijdstip'])
                                    nrValidMeas += 1
                                else:
                                    nrInvalidMeas += 1

                        if len(meetwaarden) > 0:
                            maxValue = max(meetwaarden, key=float)
                            minValue = min(meetwaarden)

                            if EIData['valueProcessingMethodCode'] in ['JGM', 'MAX']:
                                avg = sum(meetwaarden) / float(len(meetwaarden))
                            elif EIData['valueProcessingMethodCode'] == 'P90':
                                avg = np.percentile(meetwaarden, 90) # gives the 90th (linear interpolated) percentile
                            else:
                                avg = -9999
                        else:
                            avg = -9999
                            maxValue = -9999
                            minValue = -9999

                        avgYears.append(avg)
                        totalNrMeas = nrValidMeas + nrInvalidMeas

                        if len(meettijden) > 0:
                            firstObsDate = meettijden[0]
                            lastObsDate = meettijden[-1]
                        else:
                            firstObsDate = -9999
                            lastObsDate = -9999

                        EIYearDict = {
                            # EI-toets specific info
                            'year': year,
                            'avg': avg,
                            'totalNrMeas': totalNrMeas,
                            'nrInvalidMeas': nrInvalidMeas,
                            'nrValidMeas': nrValidMeas,
                            'validMeasValues': meetwaarden,
                            'validMeasTimes': meettijden,
                            'maxValue': maxValue,
                            'minValue': minValue,
                            'startTimeReq': starttimeDDString,
                            'endTimeReq': endtimeDDString,
                            'firstObsDate': firstObsDate,
                            'lastObsDate': lastObsDate,
                            'measLimit': measLimit
                        }

                        EIYearData.append(EIYearDict)

                    # compute average over all years
                    if len(avgYears) > 0:
                        EIData['avg'] = sum(avgYears) / float(len(avgYears))
                    else:
                        EIData['avg'] = 0

                    EIData['yearData'] = EIYearData
                    logger.info("Succesfully calculated average value of: " + str(EIData['avg']))

                #endregion

                #region Store all non-EI data of this location&parameter to the MongoDB as a GeoJSON feature

                # only store data if an average is computed and if measurement is a 'Steekmonster'
                if EIData['bemonsteringsSoortOmschrijving'] != "Steekmonster":
                    print "Data is not stored, bemonstering is: " + EIData['bemonsteringsSoortOmschrijving']

                if 'avg' in EIData.keys() and EIData['bemonsteringsSoortOmschrijving'] == "Steekmonster":

                    pointLoc = transformPoint(location['X'], location['Y'], inputEPSG, outputEPSG)
                    locCoords = [pointLoc[0], pointLoc[1]]

                    result = {
                        'type': 'Feature',
                        'geometry': {
                                'type': 'Point',
                                'coordinates': locCoords
                            },
                        'properties': {
                            # default obligatory properties
                            'source': 'DDL',
                            'sourceDesc': 'Gegevens uit de data distributielaag van Rijkswaterstaat',
                            'aquoParOmschrijving': aquometadata['Parameter']['Omschrijving'],
                            'aquoParCode': parCode,
                            'parDescription': aquometadata['Parameter_Wat_Omschrijving'],
                            'locID': locID,
                            'locName': location['Naam'],

                            'EIData': EIData,

                            # Source specific properties
                            'sourceProp': {'X': repr(location['X']),
                                           'Y': repr(location['Y'])
                            }
                        }
                    } # end result

                    post_id = collection.insert_one(result).inserted_id

                    n += 1
                    print "Finished computations: " + str(n)

            #endregion

print "Script done"



# merge averages if data is spread in multiple time periods


# #read existing data in database and store parameter + loc codes as unique identifier
# mongocursor = collection.find()
#
# loadedTimeSeries = []
#
# for record in mongocursor:
#
#     for yearData in record['properties']['EIData']['yearData']:
#
#         parCode = record['properties']['aquoParCode']
#         locID = record['properties']['locID']
#         year = yearData['year']
#
#         loadedTimeSeries.append(parCode + "_" + locID + "_" + str(year))
#
#
# len(loadedTimeSeries)
#
#
#
# a = {'year':[1,2,3]}
# b = {'year':[4,5,3]}
# b.items()
#
# c = dict(a.items() + b.items() +  [(k, a[k] + b[k]) for k in set(b) & set(a)])




