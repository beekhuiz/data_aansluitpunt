import os
from flask import Flask, Response, jsonify, request, render_template, session, redirect, url_for, flash, send_from_directory
from pymongo import MongoClient
import requests
from requests.auth import HTTPBasicAuth
#import gis_functions       # NOT NEEDED (YET)
import json
from operator import itemgetter  # used for sorting dictionary lists of unique locations, parameters and sources alphabetically

app = Flask(__name__)
app.config['SECRET_KEY'] = 'gukfdshkjdsoipee'
my_dir = os.path.dirname(__file__)

app.config['DEVELOP'] = True

# Load in norm data in memory for fast access
RIVMString = requests.get('https://rvs.rivm.nl/zoeksysteem/Data/SubtanceNormValues')
RIVMDict = json.loads(RIVMString.text)  # convert json string to python dict
normsList = RIVMDict['norms']
substancesList = RIVMDict['substances']


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/norms')
def getNorms():

    if 'parCode' in request.args.keys():
        parCode = request.args['parCode']

        allInfo = {}

        normsForSubstance = []

        # find the norm id of this substance by aquocode
        for substance in substancesList:
            if substance['aquoCode'] == parCode:

                #region store info of this substance
                allInfo['aquoCode'] = substance['aquoCode']
                allInfo['name'] = substance['name']
                allInfo['englishName'] = substance['englishName']
                allInfo['casNumber'] = substance['casNumber']
                allInfo['hasZzsEntry'] = substance['hasZzsEntry']
                #endregion

                # go through all norms of this substance
                norms = substance['norms']

                for norm in norms:

                    normData = {}
                    normData['value'] = norm['value']

                    #region get the norm ID and retrieve all info of the norm ID out of the norms dictionary
                    normID = norm['id']
                    normInfo = {}

                    for norm in normsList:
                        if norm['id'] == normID:
                            normInfo['id'] = norm['id']
                            normInfo['description'] = norm['description']
                            normInfo['compartmentName'] = norm['compartmentName']
                            normInfo['categoryDescription'] = norm['categoryDescription']
                            normInfo['normCode'] = norm['normCode']
                            normInfo['normDescription'] = norm['normDescription']
                            normInfo['normSubgroupCode'] = norm['normSubgroupCode']
                            normInfo['normSubgroupDescription'] = norm['normSubgroupDescription']
                            normInfo['compartmentCode'] = norm['compartmentCode']
                            normInfo['compartmentDescription'] = norm['compartmentDescription']
                            normInfo['compartmentSubgroupCode'] = norm['compartmentSubgroupCode']
                            normInfo['compartmentSubgroupDescription'] = norm['compartmentSubgroupDescription']
                            normInfo['quantityCode'] = norm['quantityCode']
                            normInfo['quantityDescription'] = norm['quantityDescription']
                            normInfo['stateCode'] = norm['stateCode']
                            normInfo['stateDescription'] = norm['stateDescription']
                            normInfo['valueProcessingMethodCode'] = norm['valueProcessingMethodCode']
                            normInfo['valueProcessingMethodDescription'] = norm['valueProcessingMethodDescription']

                    normData['info'] = normInfo
                    #endregion

                    normsForSubstance.append(normData)

        allInfo['norms'] = normsForSubstance

        resp = Response(json.dumps(allInfo))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.mimetype = 'application/json'
        return resp

    else:
        return "Please give a valid aquo code 'parCode' as GET parameter"



@app.route('/locations')
def getLocations():

    searchList = []

    if 'parCode' in request.args.keys():
        searchList.append({"properties.aquoParCode": request.args['parCode']})

    if 'locID' in request.args.keys():
        searchList.append({"properties.locID": request.args['locID']})

    searchDict = {}
    if searchList:
        searchDict["$and"] = searchList

    client = MongoClient()
    db = client.EI_Toets
    collection = db["EIData"]

    mongocursor = collection.find(searchDict)

    timeseries = []

    for record in mongocursor:
        del record['_id'] # remove mongoID, that should not be part of the output (and is not JSON Serializable)
        timeseries.append(record)

    uniqLocIDSeries = list({v['properties']['locID']: v for v in timeseries}.values())

    uniqLocList = []
    for uniqLocIDSerie in uniqLocIDSeries:
        uniqLoc = {}
        uniqLoc['type'] = 'Feature'
        uniqLoc['properties'] = {}
        uniqLoc['properties']['source'] = uniqLocIDSerie['properties']['source']
        uniqLoc['properties']['locID'] = uniqLocIDSerie['properties']['locID']
        uniqLoc['properties']['locName'] = uniqLocIDSerie['properties']['locName']
        uniqLoc['geometry'] = uniqLocIDSerie['geometry']
        uniqLocList.append(uniqLoc)

    uniqLocList.sort(key=lambda e: e['properties']['locName'])  # sort alphabetically on locName

    uniqLocationsDict = {}
    uniqLocationsDict['type'] = "FeatureCollection"
    uniqLocationsDict['features'] = uniqLocList

    resp = Response(json.dumps(uniqLocationsDict))
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.mimetype = 'application/json'
    return resp


@app.route('/parameters')
def getParameters():

    client = MongoClient()
    db = client.EI_Toets
    collection = db["EIData"]

    searchDict = {} # potential for selecting a subset
    mongocursor = collection.find(searchDict)

    timeseries = []

    for record in mongocursor:

        del record['_id'] # remove mongoID, that should not be part of the output (and is not JSON Serializable)
        timeseries.append(record)

    uniqParCodeSeries = list({v['properties']['aquoParCode']: v for v in timeseries}.values())
    uniqParList = []
    for uniqParCodeSerie in uniqParCodeSeries:
        uniqParList.append({'aquoParCode': uniqParCodeSerie['properties']['aquoParCode'],
                            'aquoParOmschrijving': uniqParCodeSerie['properties']['aquoParOmschrijving'],
                            'parDescription': uniqParCodeSerie['properties']['parDescription']})
    uniqParList = sorted(uniqParList, key=itemgetter('aquoParOmschrijving'))  # sort alphabetically

    #return json.dumps(uniqParList)
    resp = Response(json.dumps(uniqParList))
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.mimetype = 'application/json'
    return resp


@app.route('/avg')
def getAverage():

    searchList = []

    if 'parCode' in request.args.keys():
        searchList.append({"properties.aquoParCode": request.args['parCode']})

    if 'locID' in request.args.keys():
        searchList.append({"properties.locID": request.args['locID']})

    if searchList:
        client = MongoClient()
        db = client.EI_Toets
        collection = db["EIData"]

        searchDict = {} # potential for selecting a subset
        searchDict["$and"] = searchList

        mongocursor = collection.find(searchDict)

        timeseries = []
        for record in mongocursor:
            del record['_id'] # remove mongoID, that should not be part of the output (and is not JSON Serializable)
            timeseries.append(record)

        resp = Response(json.dumps(timeseries))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.mimetype = 'application/json'
        return resp
    else:
        return "Please give a parCode and/or a locID as request parameters"


if __name__ == '__main__':

    if app.config['DEVELOP']:
        app.run(debug=True)                 # DEVELOPMENT
    else:
        app.run(host='0.0.0.0')            # SERVER


