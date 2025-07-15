import paho.mqtt.client as mqtt
import json
import uuid
import psycopg2
import ssl
from datetime import datetime

# MQTT settings
MQTT_BROKER = "mqtt.locacoeur.com"  # Or "91.134.90.10" if using your VPS
MQTT_PORT = 8883
MQTT_TOPICS = [("LC1/+/event/#", 0), ("LC1/+/command/#", 0), ("LC1/+/result", 0)]
MQTT_CLIENT_ID = f"locacoeur-client-{uuid.uuid4()}"
MQTT_CA_CERT = "certs/ca.crt"  # Path to CA certificate
MQTT_CLIENT_CERT = "certs/client.crt"  # Path to client certificate
MQTT_CLIENT_KEY = "certs/client.key"  # Path to client private key

# Database settings
DB_CONFIG = {
    "dbname": "mqtt_db",
    "user": "mqtt_user",
    "password": "SupportLocacoeur2025!",
    "host": "91.134.90.10",
    "port": "5432"
}

# Connect to PostgreSQL
def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

# Insert data into the database
def insert_data(device_serial, topic, data):
    conn = connect_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        
        # Handle timestamp conversion (database expects bigint Unix timestamp)
        timestamp_value = data.get("timestamp")
        if timestamp_value:
            # If timestamp is already a number (Unix timestamp), use it directly
            if isinstance(timestamp_value, (int, float)):
                timestamp_value = int(timestamp_value)
            # If timestamp is a string, try to parse it and convert to Unix timestamp
            elif isinstance(timestamp_value, str):
                try:
                    # Try to parse ISO format timestamp and convert to Unix timestamp
                    dt = datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
                    timestamp_value = int(dt.timestamp())
                except ValueError:
                    # If parsing fails, use current Unix timestamp
                    print(f"Invalid timestamp format: {timestamp_value}, using current time")
                    timestamp_value = int(datetime.now().timestamp())
            else:
                # If it's a datetime object, convert to Unix timestamp
                timestamp_value = int(timestamp_value.timestamp())
        else:
            # If no timestamp, use current Unix timestamp
            timestamp_value = int(datetime.now().timestamp())
        
        cur.execute(
            """
            INSERT INTO device_data (
                device_serial, topic, battery, connection, defibrillator, 
                latitude, longitude, power_source, timestamp,
                led_power, led_defibrillator, led_monitoring, led_assistance,
                led_mqtt, led_environmental, payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                device_serial,
                topic,
                data.get("battery"),
                data.get("connection"),
                data.get("defibrillator"),
                data.get("location", {}).get("latitude"),
                data.get("location", {}).get("longitude"),
                data.get("power_source"),
                timestamp_value,
                data.get("led_power"),
                data.get("led_defibrillator"),
                data.get("led_monitoring"),
                data.get("led_assistance"),
                data.get("led_mqtt"),
                data.get("led_environmental"),
                json.dumps(data)
            )
        )
        conn.commit()
        print(f"Data inserted for device {device_serial} from topic {topic}")
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        cur.close()
        conn.close()

# MQTT callbacks
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print("Connected to MQTT broker")
        for topic, qos in MQTT_TOPICS:
            client.subscribe(topic, qos)
            print(f"Subscribed to {topic}")
    else:
        print(f"Connection failed with code {reason_code}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        data = json.loads(payload)
        topic_parts = msg.topic.split("/")
        device_serial = topic_parts[1] if len(topic_parts) > 1 else "unknown"
        insert_data(device_serial, msg.topic, data)
    except json.JSONDecodeError:
        print(f"Invalid JSON payload: {payload}")
    except Exception as e:
        print(f"Error processing message: {e}")

# Set up MQTT client with TLS (Fixed deprecation warning)
client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id=MQTT_CLIENT_ID, 
    protocol=mqtt.MQTTv5, 
    transport="tcp"
)
client.tls_set(
    ca_certs=MQTT_CA_CERT,
    certfile=MQTT_CLIENT_CERT,
    keyfile=MQTT_CLIENT_KEY,
    cert_reqs=ssl.CERT_REQUIRED,
    tls_version=ssl.PROTOCOL_TLSv1_2
)
client.on_connect = on_connect
client.on_message = on_message

# Connect to broker
try:
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
except Exception as e:
    print(f"MQTT connection error: {e}")
    exit(1)

# Start the loop
client.loop_forever()
