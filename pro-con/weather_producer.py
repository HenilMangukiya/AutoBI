import requests
import json
from time import sleep
from kafka import KafkaProducer
from datetime import datetime
from your_city_reader import get_selected_cities_from_cell
import os
from dotenv import load_dotenv

# === CONFIG ===
load_dotenv()
API_KEY = os.getenv("API_KEY")
AQI_API_KEY = os.getenv("AQI_API_KEY")

WEATHER_URL = 'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={key}&units=metric'
AQI_URL = 'https://api.waqi.info/feed/{city}/?token=ee4a04e800149785d4cd3d87f5647bf1'

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = "weatherdata"

def fetch_weather_data(city):
    try:
        # 🌡 Weather data
        weather_resp = requests.get(WEATHER_URL.format(city=city, key=API_KEY)).json()

        if "main" not in weather_resp:
            raise Exception(weather_resp.get("message", "Weather data not found."))

        # 🌫 AQI data
        aqi_resp = requests.get(AQI_URL.format(city=city, token=AQI_API_KEY)).json()

        # Safe extraction from AQI response
        if isinstance(aqi_resp, dict) and aqi_resp.get("status") == "ok":
            aqi_data = aqi_resp.get("data", {})
            aqi = aqi_data["aqi"] if isinstance(aqi_data, dict) and "aqi" in aqi_data else "N/A"
        else:
            aqi = "N/A"

        data = {
            "city": city,
            "timestamp": datetime.now().isoformat(),
            "temperature_c": weather_resp["main"]["temp"],
            "humidity": weather_resp["main"]["humidity"],
            "weather": weather_resp["weather"][0]["main"],
            "aqi": aqi
        }

        return data

    except Exception as e:
        print(f"⚠️ Error fetching data for {city}: {e}")
        return None


def main():
    while True:
        cities = get_selected_cities_from_cell()
        if not cities:
            print("No cities selected. Waiting...")
            sleep(30)
            continue

        for city in cities:
            data = fetch_weather_data(city)
            if data:
                producer.send(topic_name, value=data)
                print(f"✅ Sent data for {city}: {data}")
            sleep(2)  # avoid API rate limits

        print("🔁 Cycle complete. Sleeping for 60 seconds.\n")
        sleep(60)

if __name__ == "__main__":
    main()