import os
from flask import Flask, jsonify, request, render_template, session, redirect, url_for, flash, send_from_directory
from pymongo import MongoClient
import requests
#import gis_functions       # NOT NEEDED (YET)
import json
from operator import itemgetter  # used for sorting dictionary lists of unique locations, parameters and sources alphabetically

app = Flask(__name__)
app.config['SECRET_KEY'] = 'gukfdshkjdsoipee'
my_dir = os.path.dirname(__file__)

app.config['DEVELOP'] = True

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/locations')
def getLocations():

    r = request
    searchList = []
    searchDict = {} # potential for selecting a subset

    if 'parID' in request.args.keys():
        searchList.append({"properties.parID": request.args['parID']})
        searchDict["$and"] = searchList

    client = MongoClient()
    db = client.EI_Toets
    collection = db["EIdata"]

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

    return json.dumps(uniqLocationsDict)


@app.route('/parameters')
def getParameters():

    client = MongoClient()
    db = client.EI_Toets
    collection = db["EIdata"]

    searchDict = {} # potential for selecting a subset
    mongocursor = collection.find(searchDict)

    timeseries = []

    for record in mongocursor:

        del record['_id'] # remove mongoID, that should not be part of the output (and is not JSON Serializable)
        timeseries.append(record)

    uniqParIDSeries = list({v['properties']['parID']: v for v in timeseries}.values())
    uniqParList = []
    for uniqParIDSerie in uniqParIDSeries:
        uniqParList.append({'parID': uniqParIDSerie['properties']['parID'],
                            'parName': uniqParIDSerie['properties']['parName'],
                            'parDesc': uniqParIDSerie['properties']['parDescription']})
    uniqParList = sorted(uniqParList, key=itemgetter('parName'))  # sort alphabetically

    return json.dumps(uniqParList)


@app.route('/avg')
def getAverage():

    searchList = []

    if 'parID' in request.args.keys():
        searchList.append({"properties.parID": request.args['parID']})

    if 'locID' in request.args.keys():
        searchList.append({"properties.locID": request.args['locID']})

    if searchList:
        client = MongoClient()
        db = client.EI_Toets
        collection = db["EIdata"]

        searchDict = {} # potential for selecting a subset
        searchDict["$and"] = searchList

        mongocursor = collection.find(searchDict)

        timeseries = []
        for record in mongocursor:
            del record['_id'] # remove mongoID, that should not be part of the output (and is not JSON Serializable)
            timeseries.append(record)

        return json.dumps(timeseries)
    else:
        return "Please give a parID and/or a locID as request parameters"



if __name__ == '__main__':
    if app.config['DEVELOP']:
        app.run(debug=True)                 # DEVELOPMENT
    else:
        app.run(host='0.0.0.0')            # SERVER


