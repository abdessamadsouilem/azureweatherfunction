import logging
import requests
import pymongo
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus
import azure.functions as func

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()
API_KEY = os.getenv('WEATHER_API_KEY')
COSMOS_DB_USER = quote_plus(os.getenv('COSMOS_DB_USER')) 
COSMOS_DB_PASSWORD = quote_plus(os.getenv('COSMOS_DB_PASSWORD'))
COSMOS_DB_HOST = os.getenv('COSMOS_DB_HOST')
COSMOS_DB_CONNECTION_STRING = f"mongodb+srv://{COSMOS_DB_USER}:{COSMOS_DB_PASSWORD}@{COSMOS_DB_HOST}"

# List of cities
cities = ["New York", "London", "Paris", "Tokyo", "Sydney", "Cape Town", "Moscow"]

def fetch_weather_data(city):
    try:
        url = f"https://api.weatherapi.com/v1/forecast.json?q={city}&days=1&key={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Weather data fetched successfully for {city}")
        return data
    except requests.RequestException as e:
        logging.error(f"Error fetching weather data for {city}: {e}")
        return None

def store_data_in_db(data):
    try:
        client = pymongo.MongoClient(COSMOS_DB_CONNECTION_STRING)
        db = client.weather_db
        collection = db.weather_datas
        # Update the document if it exists, otherwise insert a new one
        collection.update_one({"location.name": data['location']['name']}, {"$set": data}, upsert=True)
        logging.info(f"Weather data updated in DB successfully for {data['location']['name']}")
    except pymongo.errors.PyMongoError as e:
        logging.error(f"Error updating data in DB for {data['location']['name']}: {e}")

def main(mytimer: func.TimerRequest) -> None:
    for city in cities:
        weather_data = fetch_weather_data(city)
        if weather_data:
            store_data_in_db(weather_data)

if __name__ == "__main__":
    main()
