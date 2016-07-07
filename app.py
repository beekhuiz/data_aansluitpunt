import os
from flask import Flask, current_app, Response, make_response, jsonify, request, render_template, session, redirect, url_for, flash, send_from_directory
from pymongo import MongoClient
import requests
from requests.auth import HTTPBasicAuth
#import gis_functions       # NOT NEEDED (YET)
import json
from operator import itemgetter  # used for sorting dictionary lists of unique locations, parameters and sources alphabetically
#from flask.ext.cors import CORS # somehow doesnt work?
from datetime import timedelta
from functools import update_wrapper
import ConfigParser

Config = ConfigParser.ConfigParser()
Config.read("config.ini")

app = Flask(__name__)

app.config['SECRET_KEY'] = Config.get('SectionOne', 'secret_key')
app.config['DEVELOP'] = Config.get('SectionOne', 'develop') in ['True', 'true', '1']

my_dir = os.path.dirname(__file__)

# Load in norm data in memory for fast access
RIVMString = requests.get('https://rvs.rivm.nl/zoeksysteem/Data/SubtanceNormValues')
RIVMDict = json.loads(RIVMString.text)  # convert json string to python dict
normsList = RIVMDict['norms']
substancesList = RIVMDict['substances']


def crossdomain(origin=None, methods=None, headers=None, max_age=21600, attach_to_all=True, automatic_options=True):
    """
    This function set all header information to allow for crossdomain requests
    :param origin:
    :param methods:
    :param headers:
    :param max_age:
    :param attach_to_all:
    :param automatic_options:
    :return:
    """

    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            h['Access-Control-Allow-Headers'] = 'x-requested-with'

            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator


@app.route('/', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def index():
    return render_template('index.html')


@app.route('/norms', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def getNorms():
    """
    get the norms for a substance, or get all norms
    """

    r =request

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

        return json.dumps(allInfo)
    elif request.query_string == "":    # empty query string: return all
        return json.dumps(RIVMDict)
    else:
        return "Please give a valid aquo code 'parCode' as GET parameter, or leave out the GET parameter to obtain all norms and substances"
        return json.dumps(RIVMDict)



@app.route('/locations', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def getLocations():
    """
    Get all unique timeseries location information for a certain parameter and / or locationID
    :return: JSON containing the locations
    """

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

    return json.dumps(uniqLocationsDict)


@app.route('/parameters', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def getParameters():
    """
    Get all the Aquo parameters in the database
    :return:
    """

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

    return json.dumps(uniqParList)


@app.route('/avg', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def getAverage():
    """
    get the timeseries average (including all additional information necessary to interpret the average) for a
    combination of a location and parameter
    :return:
    """

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

        return json.dumps(timeseries)

    else:
        return "Please give a parCode and/or a locID as request parameters"


if __name__ == '__main__':

    if app.config['DEVELOP']:
        app.run(debug=True)                 # DEVELOPMENT
    else:
        app.run(host='0.0.0.0')            # SERVER


