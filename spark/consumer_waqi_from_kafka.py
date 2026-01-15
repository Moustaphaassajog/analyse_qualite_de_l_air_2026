from kafka import KafkaConsumer
import json
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TOPIC = 'stations_air'

# Création du consumer Kafka
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='kafka:9092',      # Nom du service Kafka dans Docker
    group_id='waqi_consumer_group',       # Groupe de consommateurs
    auto_offset_reset='earliest',         # Commence depuis le début du topic
    enable_auto_commit=True,              # Commit automatique des offsets
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Décoder le JSON
)

logging.info(f"Consumer démarré sur le topic '{TOPIC}'")

# Boucle infinie pour consommer les messages
for message in consumer:
    try:
        data = message.value
        logging.info(f"Message reçu: {data['uid']} - {data['name']} - AQI: {data.get('aqi')}")
    except Exception as e:
        logging.error(f"Erreur traitement message: {e} | Message brut: {message.value}")


"""
#ORIGINALE

from kafka import KafkaConsumer
import json

TOPIC = 'stations_air'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consumer démarré, en attente des messages...")

for message in consumer:
    print(message.value)

"""