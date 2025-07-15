import paho.mqtt.client as mqtt
import json
import uuid
import psycopg2
import ssl
from datetime import datetime, timezone, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mqtt_client.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
        logger.error(f"Database connection error: {e}")
        return None

def parse_timestamp(timestamp_value):
    """
    Parse various timestamp formats and return a proper datetime object.
    Always returns a timezone-aware datetime in UTC.
    Detects and corrects invalid/placeholder timestamps.
    """
    current_time = datetime.now(timezone.utc)
    
    if not timestamp_value:
        logger.debug("No timestamp provided, using current time")
        return current_time
    
    parsed_dt = None
    
    # If timestamp is already a number (Unix timestamp)
    if isinstance(timestamp_value, (int, float)):
        try:
            # Handle both seconds and milliseconds timestamps
            if timestamp_value > 1e12:  # Milliseconds
                timestamp_value = timestamp_value / 1000
            parsed_dt = datetime.fromtimestamp(timestamp_value, tz=timezone.utc)
        except (ValueError, OSError) as e:
            logger.warning(f"Invalid Unix timestamp: {timestamp_value}, using current time. Error: {e}")
            return current_time
    
    # If timestamp is a string, try various parsing methods
    elif isinstance(timestamp_value, str):
        # Try ISO format first
        try:
            # Handle various ISO formats
            if timestamp_value.endswith('Z'):
                parsed_dt = datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
            elif '+' in timestamp_value or timestamp_value.endswith(('00', '30')):
                parsed_dt = datetime.fromisoformat(timestamp_value)
            else:
                # Assume UTC if no timezone info
                parsed_dt = datetime.fromisoformat(timestamp_value).replace(tzinfo=timezone.utc)
        except ValueError:
            logger.warning(f"Failed to parse ISO timestamp: {timestamp_value}")
        
        # If ISO parsing failed, try to parse as Unix timestamp string
        if parsed_dt is None:
            try:
                unix_ts = float(timestamp_value)
                if unix_ts > 1e12:  # Milliseconds
                    unix_ts = unix_ts / 1000
                parsed_dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
            except (ValueError, OSError):
                logger.warning(f"Failed to parse timestamp string as Unix: {timestamp_value}")
    
    # If it's a datetime object, ensure it's timezone-aware
    elif isinstance(timestamp_value, datetime):
        if timestamp_value.tzinfo is None:
            parsed_dt = timestamp_value.replace(tzinfo=timezone.utc)
        else:
            parsed_dt = timestamp_value.astimezone(timezone.utc)
    
    # If we couldn't parse anything, use current time
    if parsed_dt is None:
        logger.warning(f"Unable to parse timestamp: {timestamp_value} (type: {type(timestamp_value)}), using current time")
        return current_time
    
    # Check for obviously invalid timestamps (too old or too far in the future)
    min_valid_time = datetime(2024, 1, 1, tzinfo=timezone.utc)  # Don't accept timestamps before 2024
    max_valid_time = current_time + timedelta(days=1)  # Don't accept timestamps more than 1 day in the future
    
    if parsed_dt < min_valid_time:
        logger.warning(f"Timestamp too old ({parsed_dt}), likely a placeholder. Using current time instead.")
        return current_time
    elif parsed_dt > max_valid_time:
        logger.warning(f"Timestamp too far in future ({parsed_dt}), using current time instead.")
        return current_time
    
    return parsed_dt

# Insert data into the database
def insert_data(device_serial, topic, data):
    conn = connect_db()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        # Parse timestamp properly
        parsed_timestamp = parse_timestamp(data.get("timestamp"))
        
        # Extract location data safely
        location = data.get("location", {})
        if isinstance(location, dict):
            latitude = location.get("latitude")
            longitude = location.get("longitude")
        else:
            latitude = longitude = None
        
        # Log the data being inserted for debugging
        logger.debug(f"Inserting data for device {device_serial}: timestamp={parsed_timestamp}, topic={topic}")
        
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
                latitude,
                longitude,
                data.get("power_source"),
                int(parsed_timestamp.timestamp()),  # Convert to Unix timestamp (bigint)
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
        logger.info(f"Data inserted for device {device_serial} from topic {topic} with timestamp {parsed_timestamp}")
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

# MQTT callbacks
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        logger.info("Connected to MQTT broker")
        for topic, qos in MQTT_TOPICS:
            client.subscribe(topic, qos)
            logger.info(f"Subscribed to {topic}")
    else:
        logger.error(f"Connection failed with code {reason_code}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        logger.debug(f"Received message on topic {msg.topic}: {payload}")
        
        data = json.loads(payload)
        topic_parts = msg.topic.split("/")
        device_serial = topic_parts[1] if len(topic_parts) > 1 else "unknown"
        
        insert_data(device_serial, msg.topic, data)
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON payload on topic {msg.topic}: {payload}. Error: {e}")
    except Exception as e:
        logger.error(f"Error processing message from topic {msg.topic}: {e}")

def on_disconnect(client, userdata, flags, reason_code, properties=None):
    logger.warning(f"Disconnected from MQTT broker with code {reason_code}")

def on_log(client, userdata, level, buf):
    logger.debug(f"MQTT log: {buf}")

# Set up MQTT client with TLS
client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id=MQTT_CLIENT_ID, 
    protocol=mqtt.MQTTv5, 
    transport="tcp"
)

# Configure TLS
client.tls_set(
    ca_certs=MQTT_CA_CERT,
    certfile=MQTT_CLIENT_CERT,
    keyfile=MQTT_CLIENT_KEY,
    cert_reqs=ssl.CERT_REQUIRED,
    tls_version=ssl.PROTOCOL_TLS  # Use PROTOCOL_TLS instead of deprecated PROTOCOL_TLSv1_2
)

# Set callbacks
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect
client.on_log = on_log

# Connect to broker with retry logic
def connect_with_retry(max_retries=5, retry_delay=5):
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to connect to MQTT broker (attempt {attempt + 1}/{max_retries})")
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            return True
        except Exception as e:
            logger.error(f"MQTT connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                import time
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached. Exiting.")
                return False
    return False

# Main execution
if __name__ == "__main__":
    logger.info("Starting MQTT client...")
    
    if connect_with_retry():
        try:
            # Start the loop
            client.loop_forever()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
            client.disconnect()
            client.loop_stop()
    else:
        logger.error("Failed to connect to MQTT broker")
        exit(1)
