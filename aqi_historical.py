import pandas as pd
from datetime import datetime
import asyncio
import aiohttp

# Builds request url to access AQI data from OpenWeather
# Params: 
# lat: latitude of desired location
# lon: longitude of desired location
# return: request url for aqi

def request_aqi_url(lat, lon, month_start, month_end):

    aqi_url = "http://api.openweathermap.org/data/2.5/air_pollution/history"
    api_key = 'c5d22ed423af74ba40fc97b49c023304'

    request_url = f"{aqi_url}?lat={str(lat)}&lon={str(lon)}&start={str(month_start)}&end={str(month_end)}&appid={api_key}"

    return request_url

def hour_rounder(t):
    # Rounds down to the nearest hour
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour))

async def fetch(session, url, trt):
    async with session.get(url) as response:
        json_response = await response.json(content_type=None)
        json_response.update({'TRT_ID': trt})
        return json_response


async def main_aqi(aqi_url_list):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url['url'], trt['TRT_ID']) for url, trt in zip(aqi_url_list, aqi_url_list)]
        return await asyncio.gather(*tasks)

def aqi_historical_to_parquet(response_list):
    dn = pd.json_normalize(response_list, 'list', ['coord', ['coord', 'lon'], ['coord', 'lat'], 'TRT_ID'])
    dn = dn.drop("coord", axis=1)
    dn['dt'] = dn['dt'].apply(lambda t: hour_rounder(datetime.utcfromtimestamp(t)).strftime('%Y-%m-%d %H:%M:%S'))
    aqi_file_name = "AQI_Historical_Dec_2022.parquet"
    dn.to_parquet(aqi_file_name, index=False)

def main():

    trt_data = pd.read_csv('dimTRTstore.csv', header=0)

    month_start = int(datetime(2022,12,1,1,0).timestamp())
    month_end = int(datetime(2023,1,1,0,59,59).timestamp())

    aqi_url_list = []
    for trt, lat, lon in zip(trt_data["TRT_ID"], trt_data["Latitude"], trt_data["Longitude"]):
        aqi_url_list.append({'url': request_aqi_url(lat, lon, month_start, month_end), 'TRT_ID': trt})

    
    result_aqi = asyncio.run(main_aqi(aqi_url_list))

    aqi_historical_to_parquet(result_aqi)

if __name__ == "__main__":
    main()

