import pandas as pd
from datetime import datetime

import asyncio
import aiohttp

# Builds request url to access weather data from OpenWeather
# Params: 
# lat: latitude of desired location
# lon: longitude of desired location
# return: request url for weather

def request_weather_url(lat,lon):

    options = {}
    options["weather_url"] = "https://api.openweathermap.org/data/2.5/weather"
    options["units"] = "metric"
    options["api_key"] = 'c5d22ed423af74ba40fc97b49c023304'

    REQUEST_URL = options["weather_url"] + "?lat=" \
    + str(lat) \
    + "&lon=" + str(lon) \
    + "&appid=" + options["api_key"] \
    + "&units=" + options["units"]

    return REQUEST_URL

async def fetch(session, url):
    async with session.get(url) as response:
        json_response = await response.json(content_type=None)
        return json_response

async def main_aqi(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        return await asyncio.gather(*tasks)

def hour_rounder(t):
    # Rounds down to the nearest hour
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour))

def weather_to_csv(result):
    dw = pd.json_normalize(result, 'weather', record_prefix='weather.')
    dn = pd.json_normalize(result)
    #dn['dt'] = dn['dt'].apply(lambda t: hour_rounder(datetime.utcfromtimestamp(t)).strftime('%Y-%m-%d %H:%M:%S'))
    #dn['sys.sunrise'] = dn['sys.sunrise'].apply(lambda t: datetime.utcfromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S'))
    #dn['sys.sunset'] = dn['sys.sunset'].apply(lambda t: datetime.utcfromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S'))
    dn = dn.drop('weather', axis=1)
    df_update = pd.concat([dn,dw], axis=1)
    df_update = df_update[df_update['base'].notna()]
    weather_file_name = "Weather" + datetime.now().strftime("_%Y%m%d%H%M%S.csv")
    df_update.to_csv(weather_file_name, index=False)

def main():

    trt_data = pd.read_csv('dimTRTstore.csv', header=0)

    WEATHER_URL_LIST = []
    for lat,lon in zip(trt_data["Latitude"], trt_data["Longitude"]):
        WEATHER_URL_LIST.append(request_weather_url(lat,lon))

    
    result_aqi = asyncio.run(main_aqi(WEATHER_URL_LIST))

    weather_to_csv(result_aqi)

if __name__ == "__main__":
    main()