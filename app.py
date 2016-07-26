import os, sys
from flask import Flask, current_app, make_response, request, render_template
import pymongo
import requests
import json
from operator import itemgetter  # used for sorting dictionary lists of unique locations, parameters and sources alphabetically
from datetime import timedelta
from functools import update_wrapper
import ConfigParser
import atexit
from apscheduler.schedulers.background import BackgroundScheduler

sched = BackgroundScheduler()
RIVMDict = {}
normsList = []
substancesList = []

# Define the function that is to be executed
def updateRIVMDB():

    print("Reading RIVM DATA")

    # set RIVMDict, norms and substanceslist to global; we want to change the global variables
    global RIVMDict
    global normsList
    global substancesList

    # Retrieve the latest RIVM Norm database when starting the script
    r = requests.get('https://rvs.rivm.nl/zoeksysteem/Data/SubtanceNormValues')

    if r.status_code == 200:
        fo = open('RIVMNormDB.json', 'w')
        fo.write(r.content)
        fo.close()
    else:
        print "Error in retrieving RIVM Norm database"

    # load in the RIVM norm database
    try:
        with open('RIVMNormDB.json') as RIVMDataFile:
            RIVMDict = json.load(RIVMDataFile)  # convert json string to python dict
            normsList = RIVMDict['norms']
            substancesList = RIVMDict['substances']
    except:
        print "Error in loading RIVM norms database, exiting"
        sys.exit()


# Explicitly kick off the background thread
sched.add_job(updateRIVMDB, 'interval', id='rivm_dbupdate_id', days=7, start_date='2016-07-24 03:30:00')
sched.start()

# load the data at the start
updateRIVMDB()

Config = ConfigParser.ConfigParser()
Config.read("config.ini")

app = Flask(__name__)

app.config['SECRET_KEY'] = Config.get('SectionOne', 'secret_key')
app.config['DEVELOP'] = Config.get('SectionOne', 'develop') in ['True', 'true', '1']

my_dir = os.path.dirname(__file__)



def crossdomain(origin=None, methods=None, headers=None, max_age=21600, attach_to_all=True, automatic_options=True):
    """
    This function set all header information to allow for crossdomain requests
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


# Shutdown the scheduler thread if the web process is stopped;
atexit.register(lambda: sched.shutdown(wait=False))


if __name__ == '__main__':

    # connect to database
    client = pymongo.MongoClient(serverSelectionTimeoutMS=1)
    db = client.EI_Toets
    collection = db["EIData"]

    # test if connection to MongoDB works
    try:
        client.server_info()
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)
        print "Error in connecting or creating MongoDB collection; have you started MongoDB?"
        sys.exit()


    # run the app with use_reloader=False to ensure that apscheduler is not run twice
    if app.config['DEVELOP']:
        app.run(debug=True, use_reloader=False)                # DEVELOPMENT
    else:
        app.run(host='0.0.0.0', use_reloader=False)            # SERVER


