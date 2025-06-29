from kafka import KafkaConsumer
import json
import time

# === CONFIG ===
TOPIC = "weatherdata"
BOOTSTRAP_SERVERS = "localhost:9092"

# === INIT CONSUMER ===
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='latest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("🛰️ Listening to topic 'weatherdata'...\n")

# === CONSUME LOOP ===
for message in consumer:
    data = message.value
    print("\n=== Weather & AQI Update ===")
    
    try:
        weather = data["weather"]
        air = data["air_quality"]
        
        print(f"City        : {data['city']}")
        print(f"Time        : {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data['timestamp']))}")
        print(f"Temp (°C)   : {weather['main']['temp']}")
        print(f"Condition   : {weather['weather'][0]['description'].title()}")
        print(f"Humidity    : {weather['main']['humidity']}%")
        print(f"AQI Index   : {air['list'][0]['main']['aqi']}")
        print("=" * 40)

    except KeyError as e:
        print(f"[✘] Missing key: {e}")
        print("[ℹ] Raw data:")
        print(json.dumps(data, indent=2))
        print("=" * 40)
