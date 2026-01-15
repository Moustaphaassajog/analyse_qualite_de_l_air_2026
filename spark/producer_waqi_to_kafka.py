import requests
import time
import json
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

# Charger le token depuis .env
load_dotenv()
TOKEN = os.getenv("WAQI_TOKEN")
TOPIC = "stations_air"

# Configuration Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Liste des rectangles pour couvrir la France entière
# Format : [lat_min, lng_min, lat_max, lng_max]
RECTANGLES = [
    [41.0, -5.5, 45.0, 0.0],   # Sud-Ouest
    [41.0, 0.0, 45.0, 3.0],    # Sud
    [41.0, 3.0, 45.0, 9.8],    # Sud-Est
    [45.0, -5.5, 48.5, 0.0],   # Centre-Ouest
    [45.0, 0.0, 48.5, 3.0],    # Centre
    [45.0, 3.0, 48.5, 9.8],    # Centre-Est
    [48.5, -5.5, 51.5, 3.0],   # Nord-Ouest
    [48.5, 3.0, 51.5, 9.8],    # Nord-Est
]

# Fonction pour récupérer toutes les stations dans un rectangle
def get_stations(latlng):
    url = "https://api.waqi.info/map/bounds/"
    params = {"token": TOKEN, "latlng": latlng, "networks": "all"}
    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        if data["status"] != "ok":
            print("Erreur récupération stations !")
            return []
        return data["data"]
    except Exception as e:
        print("Erreur API get_stations:", e)
        return []

# Fonction pour récupérer les polluants d’une station
def get_pollutants(uid):
    url = f"https://api.waqi.info/feed/@{uid}/"
    params = {"token": TOKEN}
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        if data["status"] != "ok":
            return None
        iaqi = data["data"].get("iaqi", {})
        return {
            "uid": uid,
            "name": data["data"]["city"]["name"],
            "lat": data["data"]["city"]["geo"][0],
            "lon": data["data"]["city"]["geo"][1],
            "aqi": data["data"].get("aqi"),
            "dominant_pollutant": data["data"].get("dominentpol"),
            "pm25": iaqi.get("pm25", {}).get("v"),
            "pm10": iaqi.get("pm10", {}).get("v"),
            "no2": iaqi.get("no2", {}).get("v"),
            "o3": iaqi.get("o3", {}).get("v"),
            "so2": iaqi.get("so2", {}).get("v"),
            "co": iaqi.get("co", {}).get("v"),
            "time": data["data"]["time"]["s"]
        }
    except Exception as e:
        print(f"Erreur polluants uid={uid} :", e)
        return None

# Boucle infinie pour ingestion continue
while True:
    total_stations = 0
    for rect in RECTANGLES:
        latlng = f"{rect[0]},{rect[1]},{rect[2]},{rect[3]}"
        stations = get_stations(latlng)
        total_stations += len(stations)
        print(f"Rectangle {latlng} : {len(stations)} stations récupérées")
        
        for station in stations:
            info = get_pollutants(station["uid"])
            if info:
                producer.send(TOPIC, value=info)
            time.sleep(0.5)  # Pause entre chaque station pour respecter l'API

    print(f"Cycle terminé ✅ - {total_stations} stations traitées. Attente 30s")
    time.sleep(30)
    