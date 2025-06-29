import requests
import json
from time import sleep
from kafka import KafkaProducer
from datetime import datetime
from your_city_reader import get_selected_cities_from_b2

# === CONFIG ===
API_KEY = "ee4a04e800149785d4cd3d87f5647bf1"  # 🔁 Replace this with your actual OpenWeatherMap key
WEATHER_URL = 'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={key}&units=metric'
AQI_URL = 'https://api.waqi.info/feed/{city}/?token=YOUR_AQI_API_KEY'

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = "weatherdata"

def fetch_weather(city):
    try:
        w_response = requests.get(WEATHER_URL.format(city=city, key=API_KEY)).json()
        aqi_response = requests.get(AQI_URL.format(city=city)).json()

        data = {
            "city": city,
            "timestamp": datetime.now().isoformat(),
            "temperature_c": w_response["main"]["temp"],
            "humidity": w_response["main"]["humidity"],
            "weather": w_response["weather"][0]["main"],
            "aqi": aqi_response["data"].get("aqi", "N/A")
        }

        return data

    except Exception as e:
        print(f"⚠️ Error fetching data for {city}: {e}")
        return None

def main():
    while True:
        cities = get_selected_cities_from_b2()
        if not cities:
            print("No cities selected. Waiting...")
            sleep(30)
            continue

        for city in cities:
            data = fetch_weather(city)
            if data:
                producer.send(topic_name, value=data)
                print(f"✅ Sent data for {city}: {data}")
            sleep(2)  # avoid API rate limits

        print("🔁 Cycle complete. Sleeping for 60 seconds.\n")
        sleep(60)

if __name__ == "__main__":
    main()