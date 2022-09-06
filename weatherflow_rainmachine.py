from re import T
import requests
import json
import os
import time

SOURCE_BASE_URL = os.environ.get('SOURCE_BASE_URL')
SOURCE_KEY = os.environ.get('SOURCE_KEY')
SOURCE_STATION = os.environ.get('SOURCE_STATION')

ENDPOINTS = os.environ.get('ENDPOINTS')
TARGET_BASE_URL = os.environ.get('TARGET_BASE_URL')
TARGET_PORT = os.environ.get('TARGET_PORT')
TARGET_PWD = os.environ.get('TARGET_PWD')

ATLAS_TARGET_BASE_URL = os.environ.get('ATLAS_TARGET_BASE_URL')
ATLAS_TARGET_KEY = os.environ.get('ATLAS_TARGET_KEY')
ATLAS_TARGET_NAME = os.environ.get('ATLAS_TARGET_NAME')
ATLAS_TARGET_COLLECTION = os.environ.get('ATLAS_TARGET_COLLECTION')
ATLAS_TARGET_DATABASE = os.environ.get('ATLAS_TARGET_DATABASE')

def weatherflow_fetch():
    url = "{}{}/?token={}".format(SOURCE_BASE_URL, SOURCE_STATION, SOURCE_KEY)
    response = requests.get(url)
    d = response.json()
    #print(json.dumps(d['obs'][0]))
    return d

def payload_format(payload):
   
    d = {'weather': [
            {
                "mintemp": payload['obs'][0]['air_temperature'],
                "maxtemp": payload['obs'][0]['air_temperature'],
                "temperature": payload['obs'][0]['air_temperature'],
                "wind": payload['obs'][0]['wind_avg'],
                "solarrad": float(0.0),
                "qpf": payload['obs'][0]['precip_accum_last_1hr'],
                "rain": payload['obs'][0]['precip'],
                "minrh": payload['obs'][0]['relative_humidity'],
                "maxrh": payload['obs'][0]['relative_humidity'],
                "condition": 26,
                "pressure": float(0.0),
                "dewpoint": payload['obs'][0]['dew_point']
            },
            {
                'timestamp': round(time.time()-1),
                "mintemp": payload['obs'][0]['air_temperature'],
                "maxtemp": payload['obs'][0]['air_temperature'],
                "temperature": payload['obs'][0]['air_temperature'],
                "wind": payload['obs'][0]['wind_avg'],
                "solarrad": float(0.0),
                "qpf": payload['obs'][0]['precip_accum_last_1hr'],
                "rain": payload['obs'][0]['precip'],
                "minrh": payload['obs'][0]['relative_humidity'],
                "maxrh": payload['obs'][0]['relative_humidity'],
                "condition": 26,
                "pressure": float(0.0),
                "dewpoint": payload['obs'][0]['dew_point']    
            }
    ]}
    print(json.dumps(d))
    return d

def send_to_rainmachine(payload):
    log = []
    for ip in ENDPOINTS.split(","):
        # first let's auth
        url = "http://{}:{}/api/4/auth/login".format(ip, TARGET_PORT)
        u = {"pwd": TARGET_PWD, "remember": True }
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=json.dumps(u))
        access_token = json.loads(response.text)['access_token']

        # send data
        url = "http://{}:{}{}?access_token={}".format(ip, TARGET_PORT, TARGET_BASE_URL, access_token)
        response = requests.post(url, data=json.dumps(payload))
        print(response.status_code)
        log.append({'ip': ip, 'response': response.status_code})
    return(log)

def send_to_atlas(payload):
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Request-Headers': '*',
        'api-key': ATLAS_TARGET_KEY
    }
    raw_data = json.dumps({
            "dataSource": ATLAS_TARGET_NAME,
            "database": ATLAS_TARGET_DATABASE,
            "collection": ATLAS_TARGET_COLLECTION,
            "document": payload
    })
    response = requests.post(ATLAS_TARGET_BASE_URL, headers=headers, data=raw_data)
    if response.status_code != 201:
        raise ValueError("Error code was {}".format(response.status_code))
    print("flushed to atlas")

def main():
    while True:
        try:
            d = weatherflow_fetch()
            f = payload_format(d)
            r = send_to_rainmachine(f)

            log = {
                'rainmachine': {'rainmachine_response': r, 'rainmachine_data': f},
                'weatherflow': d,
                'station': d['station_id'],
                'station_name': d['station_name'],
                'station_lat': d['latitude'],
                'station_lon': d['longitude'],
                'logtime': round(time.time())
            }
            send_to_atlas(log)

        except Exception as e:
            print("ERROR: unable to process data {}".format(e))
        time.sleep(600)

if __name__ == "__main__":
    main()
