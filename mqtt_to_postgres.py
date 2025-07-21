import paho.mqtt.client as mqtt
import json
import uuid
import psycopg2
import ssl
import smtplib
import signal
import sys
import time
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate
import logging
import threading
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mqtt_client.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create separate logger for timestamp debugging
timestamp_logger = logging.getLogger('timestamp_debug')
timestamp_handler = logging.FileHandler('timestamp_debug.log')
timestamp_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
timestamp_logger.addHandler(timestamp_handler)
timestamp_logger.setLevel(logging.DEBUG)

# MQTT settings
MQTT_BROKER = "mqtt.locacoeur.com"
MQTT_PORT = 8883
MQTT_TOPICS = [("LC1/+/event/#", 0), ("LC1/+/command/#", 0), ("LC1/+/result", 0)]
MQTT_CLIENT_ID = f"locacoeur-client-{uuid.uuid4()}"
MQTT_CA_CERT = "certs/ca.crt"
MQTT_CLIENT_CERT = "certs/client.crt"
MQTT_CLIENT_KEY = "certs/client.key"

# Database settings
DB_CONFIG = {
    "dbname": "mqtt_db",
    "user": "mqtt_user",
    "password": "SupportLocacoeur2025!",
    "host": "91.134.90.10",
    "port": "5432"
}

# Email settings for alerts
EMAIL_CONFIG = {
    "smtp_server": "ssl0.ovh.net",  # Serveur SMTP OVH
    "smtp_port": 587,               # Port STARTTLS
    "username": "support@locacoeur.com",  # Adresse complète obligatoire
    "password": "86Hqw6O&8i*i",     # Mot de passe de la boîte mail OVH
    "from_email": "support@locacoeur.com",
    "to_emails": ["chechech@gmail.com"],
    "enabled": True  # Mets à True pour activer l'envoi
}

# Global variables for service control
running = True
client = None
reconnect_count = 0
last_connection_time = None

class MQTTService:
    def __init__(self):
        self.client = None
        self.running = True
        self.reconnect_count = 0
        self.last_connection_time = None
        self.setup_signal_handlers()
        
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        if self.client:
            self.client.disconnect()
            self.client.loop_stop()
        sys.exit(0)

    def connect_db(self) -> Optional[psycopg2.extensions.connection]:
        """Connect to PostgreSQL database with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                logger.debug(f"Database connection established (attempt {attempt + 1})")
                return conn
            except Exception as e:
                logger.error(f"Database connection error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    self.send_alert_email("Database Connection Failed", f"Failed to connect to database after {max_retries} attempts: {e}")
        return None

    def send_alert_email(self, subject: str, message: str, priority: str = "normal"):
        """Send email alert with retry logic"""
        if not EMAIL_CONFIG["enabled"]:
            logger.debug(f"Email alerts disabled. Would send: {subject}")
            return
            
        max_retries = 3
        for attempt in range(max_retries):
            try:
                msg = MIMEMultipart()
                msg['From'] = EMAIL_CONFIG["from_email"]
                msg['To'] = ", ".join(EMAIL_CONFIG["to_emails"])
                msg['Date'] = formatdate(localtime=True)
                msg['Subject'] = f"[LOCACOEUR-{priority.upper()}] {subject}"
                
                body = f"""
LOCACOEUR MQTT Client Alert

Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
Priority: {priority.upper()}
Subject: {subject}

Details:
{message}

Reconnection count: {self.reconnect_count}
Last connection: {self.last_connection_time}

This is an automated message from the LOCACOEUR MQTT monitoring system.
                """
                
                msg.attach(MIMEText(body, 'plain'))
                
                server = smtplib.SMTP(EMAIL_CONFIG["smtp_server"], EMAIL_CONFIG["smtp_port"])
                server.starttls()
                server.login(EMAIL_CONFIG["username"], EMAIL_CONFIG["password"])
                server.send_message(msg)
                server.quit()
                
                logger.info(f"Alert email sent successfully: {subject}")
                return
                
            except Exception as e:
                logger.error(f"Failed to send email alert (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
        
        logger.error(f"Failed to send email alert after {max_retries} attempts")

    def parse_timestamp(self, timestamp_value: Any, device_serial: str) -> datetime:
        """
        Parse various timestamp formats and return a proper datetime object.
        Always returns a timezone-aware datetime in UTC.
        Detects and corrects invalid/placeholder timestamps.
        """
        current_time = datetime.now(timezone.utc)
        original_timestamp = timestamp_value
        
        # Log original timestamp for debugging
        timestamp_logger.debug(f"Device {device_serial}: Original timestamp = {original_timestamp} (type: {type(original_timestamp)})")
        
        if not timestamp_value:
            logger.debug(f"Device {device_serial}: No timestamp provided, using current time")
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
                logger.warning(f"Device {device_serial}: Invalid Unix timestamp: {original_timestamp}, using current time. Error: {e}")
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
                logger.warning(f"Device {device_serial}: Failed to parse ISO timestamp: {original_timestamp}")
            
            # If ISO parsing failed, try to parse as Unix timestamp string
            if parsed_dt is None:
                try:
                    unix_ts = float(timestamp_value)
                    if unix_ts > 1e12:  # Milliseconds
                        unix_ts = unix_ts / 1000
                    parsed_dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                except (ValueError, OSError):
                    logger.warning(f"Device {device_serial}: Failed to parse timestamp string as Unix: {original_timestamp}")
        
        # If it's a datetime object, ensure it's timezone-aware
        elif isinstance(timestamp_value, datetime):
            if timestamp_value.tzinfo is None:
                parsed_dt = timestamp_value.replace(tzinfo=timezone.utc)
            else:
                parsed_dt = timestamp_value.astimezone(timezone.utc)
        
        # If we couldn't parse anything, use current time
        if parsed_dt is None:
            logger.warning(f"Device {device_serial}: Unable to parse timestamp: {original_timestamp} (type: {type(original_timestamp)}), using current time")
            return current_time
        
        # Check for obviously invalid timestamps (too old or too far in the future)
        min_valid_time = datetime(2024, 1, 1, tzinfo=timezone.utc)  # Don't accept timestamps before 2024
        max_valid_time = current_time + timedelta(days=1)  # Don't accept timestamps more than 1 day in the future
        
        if parsed_dt < min_valid_time:
            logger.warning(f"Device {device_serial}: Timestamp too old ({parsed_dt}), likely a placeholder. Using current time instead.")
            timestamp_logger.debug(f"Device {device_serial}: Replaced old timestamp {original_timestamp} -> {current_time}")
            return current_time
        elif parsed_dt > max_valid_time:
            logger.warning(f"Device {device_serial}: Timestamp too far in future ({parsed_dt}), using current time instead.")
            timestamp_logger.debug(f"Device {device_serial}: Replaced future timestamp {original_timestamp} -> {current_time}")
            return current_time
        
        timestamp_logger.debug(f"Device {device_serial}: Parsed timestamp {original_timestamp} -> {parsed_dt}")
        return parsed_dt

    def detect_critical_alerts(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Detect critical conditions and send alerts"""
        alerts = []
        critical_alert_messages = ["Defibrillator fault", "Power is cut", "Device is removed"]
        
        # Check for alert messages
        if "alert" in topic.lower():
            alert_message = data.get("message")
            if alert_message in critical_alert_messages:
                alerts.append(f"Critical alert: {alert_message} (ID: {data.get('id')})")
        
        # Check battery level
        battery = data.get("battery")
        if battery is not None and battery < 20:
            alerts.append(f"Low battery: {battery}%")
        
        # Check connection status
        connection = data.get("connection")
        if connection is not None and connection == 0:
            alerts.append("Device connection lost")
        
        # Check defibrillator status
        defibrillator = data.get("defibrillator")
        if defibrillator is not None and defibrillator == 0:
            alerts.append("Defibrillator not ready")
        
        # Send alerts if any critical conditions detected
        if alerts:
            alert_message = f"Critical alerts for device {device_serial}:\n" + "\n".join(f"- {alert}" for alert in alerts)
            alert_message += f"\n\nTopic: {topic}\nData: {json.dumps(data, indent=2)}"
            self.send_alert_email(f"Critical Alert - Device {device_serial}", alert_message, "critical")
            logger.warning(f"Critical alerts sent for device {device_serial}: {alerts}")

    def insert_data(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert data into the database"""
        conn = self.connect_db()
        if not conn:
            return
        
        try:
            cur = conn.cursor()
            parsed_timestamp = self.parse_timestamp(data.get("timestamp"), device_serial)
            location = data.get("location", {})
            latitude = location.get("latitude") if isinstance(location, dict) else None
            longitude = location.get("longitude") if isinstance(location, dict) else None
            led_power = data.get("led_power")
            led_defibrillator = data.get("led_defibrillator")
            alert_id = data.get("id") if "alert" in topic.lower() else None
            alert_message = data.get("message") if "alert" in topic.lower() else None
            
            # Update LED states based on alert ID
            if alert_id == 2:
                led_power = "red"
            elif alert_id == 3:
                led_defibrillator = "red"
            
            # Check for critical alerts before inserting
            self.detect_critical_alerts(device_serial, topic, data)
            
            logger.debug(f"Inserting data for device {device_serial}: timestamp={parsed_timestamp}, topic={topic}")
            
            cur.execute(
                """
                INSERT INTO device_data (
                    device_serial, topic, battery, connection, defibrillator, 
                    latitude, longitude, power_source, timestamp,
                    led_power, led_defibrillator, led_monitoring, led_assistance,
                    led_mqtt, led_environmental, alert_id, alert_message, payload,
                    original_timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    int(parsed_timestamp.timestamp()),
                    led_power,
                    led_defibrillator,
                    data.get("led_monitoring"),
                    data.get("led_assistance"),
                    data.get("led_mqtt"),
                    data.get("led_environmental"),
                    alert_id,
                    alert_message,
                    json.dumps(data),
                    data.get("timestamp")  # Store original timestamp for debugging
                )
            )
            conn.commit()
            logger.info(f"Data inserted for device {device_serial} from topic {topic} with timestamp {parsed_timestamp}")
            
        except Exception as e:
            logger.error(f"Database error for device {device_serial}: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        """MQTT connection callback"""
        if reason_code == 0:
            logger.info("Connected to MQTT broker")
            self.last_connection_time = datetime.now(timezone.utc)
            self.reconnect_count = 0
            
            for topic, qos in MQTT_TOPICS:
                client.subscribe(topic, qos)
                logger.info(f"Subscribed to {topic}")
                
            # Send connection success email if this is a reconnection
            if self.reconnect_count > 0:
                self.send_alert_email("MQTT Connection Restored", f"Successfully reconnected to MQTT broker after {self.reconnect_count} attempts")
        else:
            logger.error(f"Connection failed with code {reason_code}")
            self.reconnect_count += 1
            if self.reconnect_count >= 5:
                self.send_alert_email("MQTT Connection Failed", f"Failed to connect to MQTT broker after {self.reconnect_count} attempts. Reason code: {reason_code}", "critical")

    def on_message(self, client, userdata, msg):
        """MQTT message callback"""
        try:
            payload = msg.payload.decode("utf-8")
            logger.debug(f"Received message on topic {msg.topic}: {payload}")
            
            data = json.loads(payload)
            topic_parts = msg.topic.split("/")
            device_serial = topic_parts[1] if len(topic_parts) > 1 else "unknown"
            
            self.insert_data(device_serial, msg.topic, data)
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON payload on topic {msg.topic}: {payload}. Error: {e}")
        except Exception as e:
            logger.error(f"Error processing message from topic {msg.topic}: {e}")

    def on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        """MQTT disconnect callback"""
        logger.warning(f"Disconnected from MQTT broker with code {reason_code}")
        if self.running:
            self.reconnect_count += 1
            if self.reconnect_count >= 3:
                self.send_alert_email("MQTT Disconnection", f"Disconnected from MQTT broker. Reason code: {reason_code}. Reconnect attempt: {self.reconnect_count}")

    def on_log(self, client, userdata, level, buf):
        """MQTT log callback"""
        logger.debug(f"MQTT log: {buf}")

    def connect_with_retry(self, max_retries=10, retry_delay=5):
        """Connect to MQTT broker with retry logic"""
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to connect to MQTT broker (attempt {attempt + 1}/{max_retries})")
                self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
                return True
            except Exception as e:
                logger.error(f"MQTT connection attempt {attempt + 1} failed: {e}")
                self.reconnect_count = attempt + 1
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 1.5, 60)  # Exponential backoff with max 60s
                else:
                    logger.error("Max retries reached.")
                    self.send_alert_email("MQTT Connection Failed", f"Failed to connect to MQTT broker after {max_retries} attempts. Last error: {e}", "critical")
        return False

    def setup_mqtt_client(self):
        """Setup MQTT client with TLS and callbacks"""
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=MQTT_CLIENT_ID, 
            protocol=mqtt.MQTTv5, 
            transport="tcp"
        )

        # Configure TLS
        self.client.tls_set(
            ca_certs=MQTT_CA_CERT,
            certfile=MQTT_CLIENT_CERT,
            keyfile=MQTT_CLIENT_KEY,
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS
        )

        # Set callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_log = self.on_log

    def run(self):
        """Main service loop"""
        logger.info("Starting LOCACOEUR MQTT service...")
        
        self.setup_mqtt_client()
        
        if self.connect_with_retry():
            try:
                # Start the MQTT loop in a separate thread
                self.client.loop_start()
                
                # Main service loop
                while self.running:
                    # Check if we're still connected
                    if not self.client.is_connected():
                        logger.warning("MQTT connection lost, attempting to reconnect...")
                        if not self.connect_with_retry():
                            logger.error("Failed to reconnect, retrying in 30 seconds...")
                            time.sleep(30)
                            continue
                    
                    # Sleep and allow the service to process messages
                    time.sleep(1)
                    
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, shutting down...")
            finally:
                self.client.disconnect()
                self.client.loop_stop()
        else:
            logger.error("Failed to connect to MQTT broker")
            return False
        
        logger.info("LOCACOEUR MQTT service stopped")
        return True

# Main execution
if __name__ == "__main__":
    service = MQTTService()
    
    # Check if running as a service (no console attached)
    try:
        sys.stdout.write("")
        sys.stdout.flush()
        console_available = True
    except:
        console_available = False
    
    if not console_available:
        logger.info("Running as a service (no console)")
    
    try:
        service.run()
    except Exception as e:
        logger.error(f"Service crashed: {e}")
        service.send_alert_email("Service Crashed", f"MQTT service crashed with error: {e}", "critical")
        sys.exit(1)
