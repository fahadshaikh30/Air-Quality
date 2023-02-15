import pandas as pd
import sqlalchemy

import asyncio
import aiohttp

# Builds request url to access AQI data from OpenWeather
# Params: 
# lat: latitude of desired location
# lon: longitude of desired location
# return: request url for aqi

def request_aqi_url(lat,lon):

    options = {}
    options["aqi_url"] = "http://api.openweathermap.org/data/2.5/air_pollution"
    options["units"] = "metric"
    options["api_key"] = 'c5d22ed423af74ba40fc97b49c023304'

    REQUEST_URL = options["aqi_url"] + "?lat=" \
    + str(lat) \
    + "&lon=" + str(lon) \
    + "&appid=" + options["api_key"]

    return REQUEST_URL

async def fetch(session, url, trt):
    async with session.get(url) as response:
        json_response = await response.json(content_type=None)
        json_response.update({'TRT_ID': trt})
        return json_response

async def main_aqi():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url['url'], trt['TRT_ID']) for url, trt in zip(AQI_URL_LIST, AQI_URL_LIST)]
        return await asyncio.gather(*tasks)

def preprocess_response(result):
    dw = pd.json_normalize(result, 'list', record_prefix='aqi.')
    dn = pd.json_normalize(result)
    dn = dn.drop('list', axis=1)
    df = pd.concat([dn,dw], axis=1)
    return df

def main():

    trt_data = pd.read_csv('dimTRTstore.csv', header=0)

    global AQI_URL_LIST 
    AQI_URL_LIST = []

    for trt,lat,lon in zip(trt_data["TRT_ID"],trt_data["Latitude"], trt_data["Longitude"]):
        AQI_URL_LIST.append({'url': request_aqi_url(lat,lon), 'TRT_ID': trt})

    
    result_aqi = asyncio.run(main_aqi())

    df = preprocess_response(result_aqi)

    server_name = 'sjc04ihsprddb.teslamotors.com'
    database_name = 'dAQI'
    table_name = 't_pr_ds10333_AQIHourly'
    driver = 'SQL Server'

    engine = sqlalchemy.create_engine('mssql+pyodbc://@'+server_name+'/'+database_name+'?trusted_connection=yes&driver='+driver, fast_executemany=True)

    df.to_sql(table_name, engine, if_exists='append', schema='dbo',index=False)

if __name__ == "__main__":
    main()