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
    "to_emails": ["alertHousse@proton.me"],
    "enabled": True  # Mets à True pour activer l'envoi
}

# Global variables for service control
running = True
client = None
reconnect_count = 0
last_connection_time = None

def setup_mqtt_client(self):
    self.client = mqtt.Client(client_id=f"locacoeur-client-{uuid.uuid4()}")
    self.client.on_connect = self.on_connect
    self.client.on_message = self.on_message
    self.client.on_disconnect = self.on_disconnect
    self.client.tls_set(
        ca_certs="C:\\Users\\SupportLocacoeur\\Documents\\GitHub\\MQTT_Database\\certs\\ca.crt",
        certfile="C:\\Users\\SupportLocacoeur\\Documents\\GitHub\\MQTT_Database\\certs\\client.crt",
        keyfile="C:\\Users\\SupportLocacoeur\\Documents\\GitHub\\MQTT_Database\\certs\\client.key",
        tls_version=ssl.PROTOCOL_TLSv1_2
    )
    self.client.username_pw_set(username="locacoeur", password="your_password")

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

    def parse_timestamp(self, timestamp_value: Any, device_serial: str, return_unix: bool = False) -> Any:
        """
        Parse various timestamp formats and return a datetime object or Unix timestamp (milliseconds).
        Always returns a timezone-aware datetime in UTC or milliseconds if return_unix=True.
        """
        current_time = datetime.now(timezone.utc)
        timestamp_logger.debug(f"Device {device_serial}: Original timestamp = {timestamp_value} (type: {type(timestamp_value)})")

        if not timestamp_value:
            logger.debug(f"Device {device_serial}: No timestamp provided, using current time")
            return int(current_time.timestamp() * 1000) if return_unix else current_time

        parsed_dt = None
        if isinstance(timestamp_value, (int, float)):
            try:
                if timestamp_value > 1e12:  # Milliseconds
                    timestamp_value = timestamp_value / 1000
                parsed_dt = datetime.fromtimestamp(timestamp_value, tz=timezone.utc)
            except (ValueError, OSError) as e:
                logger.warning(f"Device {device_serial}: Invalid Unix timestamp: {timestamp_value}, using current time. Error: {e}")
                return int(current_time.timestamp() * 1000) if return_unix else current_time
        elif isinstance(timestamp_value, str):
            try:
                if timestamp_value.endswith('Z'):
                    parsed_dt = datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
                elif '+' in timestamp_value or timestamp_value.endswith(('00', '30')):
                    parsed_dt = datetime.fromisoformat(timestamp_value)
                else:
                    parsed_dt = datetime.fromisoformat(timestamp_value).replace(tzinfo=timezone.utc)
            except ValueError:
                try:
                    unix_ts = float(timestamp_value)
                    if unix_ts > 1e12:  # Milliseconds
                        unix_ts = unix_ts / 1000
                    parsed_dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                except (ValueError, OSError):
                    logger.warning(f"Device {device_serial}: Failed to parse timestamp string as Unix: {timestamp_value}")
        elif isinstance(timestamp_value, datetime):
            if timestamp_value.tzinfo is None:
                parsed_dt = timestamp_value.replace(tzinfo=timezone.utc)
            else:
                parsed_dt = timestamp_value.astimezone(timezone.utc)

        if parsed_dt is None:
            logger.warning(f"Device {device_serial}: Unable to parse timestamp: {timestamp_value} (type: {type(timestamp_value)}), using current time")
            return int(current_time.timestamp() * 1000) if return_unix else current_time

        min_valid_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        max_valid_time = current_time + timedelta(days=1)
        if parsed_dt < min_valid_time or parsed_dt > max_valid_time:
            logger.warning(f"Device {device_serial}: Timestamp out of range ({parsed_dt}), using current time.")
            timestamp_logger.debug(f"Device {device_serial}: Replaced out-of-range timestamp {timestamp_value} -> {current_time}")
            return int(current_time.timestamp() * 1000) if return_unix else current_time

        timestamp_logger.debug(f"Device {device_serial}: Parsed timestamp {timestamp_value} -> {parsed_dt}")
        return int(parsed_dt.timestamp() * 1000) if return_unix else parsed_dt

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
        if battery is not None and isinstance(battery, (int, float)) and battery < 20:
            alerts.append(f"Low battery: {battery}%")

        # Check MQTT connection status
        led_mqtt = data.get("led_mqtt")
        if led_mqtt == "Red":
            alerts.append("Device connection lost")

        # Check defibrillator status
        led_defibrillator = data.get("led_defibrillator")
        if led_defibrillator == "Red":
            alerts.append("Defibrillator not ready")

        # Check power status
        led_power = data.get("led_power")
        if led_power == "Red":
            alerts.append("Power supply issue")

        # Send alerts if any critical conditions detected
        if alerts:
            alert_message = f"Critical alerts for device {device_serial}:\n" + "\n".join(f"- {alert}" for alert in alerts)
            alert_message += f"\n\nTopic: {topic}\nData: {json.dumps(data, indent=2)}"
            self.send_alert_email(f"Critical Alert - Device {device_serial}", alert_message, "critical")
            logger.warning(f"Critical alerts sent for device {device_serial}: {alerts}")
        else:
            logger.debug(f"No critical alerts detected for device {device_serial} on topic {topic}")
    
    def insert_device_data(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert data into the device_data table."""
        conn = self.connect_db()
        if not conn:
            return
        try:
            cur = conn.cursor()
            parsed_timestamp = self.parse_timestamp(data.get("timestamp"), device_serial, return_unix=True)
            payload_str = json.dumps(data, sort_keys=True)

            # Validate required fields
            if not device_serial or not topic:
                logger.error(f"Missing device_serial or topic in data: {data}")
                self.send_alert_email(
                    "Invalid Data",
                    f"Missing device_serial or topic for device {device_serial} on topic {topic}",
                    "warning"
                )
                return

            # Validate defibrillator value
            defibrillator = data.get("defibrillator")
            if defibrillator is not None and (not isinstance(defibrillator, int) or defibrillator < 0):
                logger.warning(f"Invalid defibrillator value for device {device_serial}: {defibrillator}. Setting to None.")
                defibrillator = None
                self.send_alert_email(
                    "Invalid Data Warning",
                    f"Device {device_serial} sent invalid defibrillator value: {defibrillator}. Set to None.",
                    "warning"
                )

            cur.execute(
                """
                INSERT INTO device_data (
                    device_serial, topic, battery, connection, defibrillator,
                    latitude, longitude, power_source, timestamp,
                    led_power, led_defibrillator, led_monitoring, led_assistance,
                    led_mqtt, led_environmental, payload, alert_id, alert_message,
                    original_timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    device_serial,
                    topic,
                    data.get("battery"),
                    data.get("connection"),
                    defibrillator,
                    data.get("latitude"),
                    data.get("longitude"),
                    data.get("power_source"),
                    parsed_timestamp,
                    data.get("led_power"),
                    data.get("led_defibrillator"),
                    data.get("led_monitoring"),
                    data.get("led_assistance"),
                    data.get("led_mqtt"),
                    data.get("led_environmental"),
                    payload_str,
                    data.get("id") if "alert" in topic.lower() else None,
                    data.get("message") if "alert" in topic.lower() else None,
                    data.get("timestamp") if isinstance(data.get("timestamp"), (int, float)) else None
                )
            )
            conn.commit()
            logger.info(f"Inserted into device_data for device {device_serial} on topic {topic}")
        except Exception as e:
            logger.error(f"Error inserting into device_data for device {device_serial}: {e}")
            conn.rollback()
            self.send_alert_email(
                "Device Data Insertion Error",
                f"Failed to insert into device_data for device {device_serial} on topic {topic}: {e}",
                "critical"
            )
        finally:
            cur.close()
            conn.close()

    def insert_data(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert data into the database based on MQTT topic"""
        conn = self.connect_db()
        if not conn:
            return

        try:
            cur = conn.cursor()
            parsed_timestamp = self.parse_timestamp(data.get("timestamp"), device_serial)
            topic_parts = topic.split("/")
            topic_category = topic_parts[2] if len(topic_parts) > 2 else None
            operation_id = topic_parts[3] if len(topic_parts) > 3 else None
            payload_str = json.dumps(data, sort_keys=True)  # Normalize JSON for comparison

            # Validate defibrillator value
            defibrillator = data.get("defibrillator")
            if defibrillator is not None and (not isinstance(defibrillator, int) or defibrillator < 0):
                logger.warning(f"Invalid defibrillator value for device {device_serial}: {defibrillator}. Setting to None.")
                data["defibrillator"] = None
                self.send_alert_email(
                    "Invalid Data Warning",
                    f"Device {device_serial} sent invalid defibrillator value: {defibrillator}. Set to None.",
                    "warning"
                )

            # Get server_id for production environment
            server_id = None
            try:
                cur.execute("SELECT server_id FROM Servers WHERE environment = %s", ("production",))
                result = cur.fetchone()
                if result:
                    server_id = result[0]
                else:
                    logger.warning("No 'production' server found in Servers table. Inserting default server.")
                    cur.execute(
                        """
                        INSERT INTO Servers (environment, mqtt_url)
                        VALUES (%s, %s)
                        RETURNING server_id
                        """,
                        ("production", "mqtts://mqtt.locacoeur.com:8883")
                    )
                    server_id = cur.fetchone()[0]
            except psycopg2.Error as e:
                logger.error(f"Error accessing Servers table: {e}. Using default server_id = 1")
                server_id = 1
                self.send_alert_email(
                    "Database Schema Error",
                    f"Failed to access Servers table: {e}. Using default server_id = 1 for device {device_serial}",
                    "critical"
                )

            # Ensure device exists
            cur.execute(
                """
                INSERT INTO Devices (serial, mqtt_broker_url)
                VALUES (%s, %s)
                ON CONFLICT (serial) DO NOTHING
                """,
                (device_serial, "mqtts://mqtt.locacoeur.com:8883")
            )

            # Handle Events (LC1/{serial}/event/#)
            if topic_category == "event":
                message_id = operation_id
                try:
                    cur.execute(
                        """
                        INSERT INTO Events (
                            device_serial, server_id, operation_id, topic, message_id, 
                            payload, event_timestamp, created_at, original_timestamp
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (device_serial, topic, payload, event_timestamp) DO UPDATE SET
                            created_at = EXCLUDED.created_at,
                            original_timestamp = EXCLUDED.original_timestamp
                        """,
                        (
                            device_serial,
                            server_id,
                            message_id,
                            topic,
                            message_id,
                            payload_str,
                            parsed_timestamp,
                            datetime.now(timezone.utc),
                            str(data.get("timestamp"))
                        )
                    )
                    
                    # Check if this was an insert or update
                    if cur.rowcount > 0:
                        logger.debug(f"Event processed for device {device_serial} on topic {topic}")
                    else:
                        logger.debug(f"Duplicate event ignored for device {device_serial} on topic {topic}")
                        
                except psycopg2.IntegrityError as e:
                    if "unique_event" in str(e):
                        logger.debug(f"Duplicate event skipped for device {device_serial} on topic {topic}")
                        # Don't rollback, just continue
                    else:
                        # Re-raise if it's a different integrity error
                        raise

                # Update LEDs table
                led_updates = {}
                alert_id = data.get("id") if message_id == "Alert" else None
                if alert_id == 2:
                    led_updates["Power"] = ("Red", "Device is running on battery power")
                elif alert_id == 3:
                    led_updates["Defibrillator"] = ("Red", "Indicates a fault in the defibrillator")
                elif message_id == "Status":
                    if "led_power" in data:
                        led_updates["Power"] = (
                            data["led_power"],
                            "Device is powered by an external power supply" if data["led_power"] == "Green" else "Device is running on battery power"
                        )
                    if "led_defibrillator" in data:
                        led_updates["Defibrillator"] = (
                            data["led_defibrillator"],
                            "Defibrillator is functioning normally" if data["led_defibrillator"] == "Green" else "Indicates a fault in the defibrillator"
                        )
                    if "led_monitoring" in data:
                        led_updates["Monitoring"] = (
                            data["led_monitoring"],
                            "Monitoring is active" if data["led_monitoring"] == "Green" else "Monitoring is disabled"
                        )
                    if "led_assistance" in data:
                        led_updates["Assistance"] = (
                            data["led_assistance"],
                            "Assistance mode is enabled" if data["led_assistance"] == "Green" else "Assistance is disabled"
                        )
                    if "led_mqtt" in data:
                        led_updates["MQTT"] = (
                            data["led_mqtt"],
                            "Connected to the MQTT broker successfully" if data["led_mqtt"] == "Green" else "Disconnected from the MQTT broker"
                        )
                    if "led_environmental" in data:
                        led_updates["Environmental"] = (
                            data["led_environmental"],
                            "Neither fan nor heater is active" if data["led_environmental"] == "Off" else "Temperature is outside safe limits"
                        )

                for led_type, (status, description) in led_updates.items():
                    cur.execute(
                        """
                        INSERT INTO LEDs (device_serial, led_type, status, description, last_updated)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (device_serial, led_type) DO UPDATE
                        SET status = EXCLUDED.status, description = EXCLUDED.description, last_updated = EXCLUDED.last_updated
                        """,
                        (device_serial, led_type, status, description, datetime.now(timezone.utc))
                    )

            # Handle Commands (LC1/{serial}/command/#)
            elif topic_category == "command":
                message_id = operation_id
                try:
                    cur.execute(
                        """
                        INSERT INTO Commands (
                            device_serial, server_id, operation_id, topic, message_id, payload, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            device_serial,
                            server_id,
                            message_id,
                            topic,
                            message_id,
                            payload_str,
                            datetime.now(timezone.utc)
                        )
                    )
                except psycopg2.IntegrityError as e:
                    logger.debug(f"Duplicate command skipped for device {device_serial} on topic {topic}")
                    # Don't rollback, just continue

            # Handle Results (LC1/{serial}/result)
            elif topic_category == "result":
                result_status = data.get("status", "Success")
                result_message = data.get("message")
                try:
                    cur.execute(
                        """
                        SELECT command_id FROM Commands
                        WHERE device_serial = %s
                        ORDER BY created_at DESC LIMIT 1
                        """,
                        (device_serial,)
                    )
                    command_id = cur.fetchone()[0] if cur.rowcount > 0 else None

                    cur.execute(
                        """
                        INSERT INTO Results (
                            command_id, device_serial, topic, result_status, result_message, payload, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            command_id,
                            device_serial,
                            topic,
                            result_status,
                            result_message,
                            payload_str,
                            datetime.now(timezone.utc)
                        )
                    )
                except psycopg2.IntegrityError as e:
                    logger.debug(f"Duplicate result skipped for device {device_serial} on topic {topic}")
                    # Don't rollback, just continue

            # Check for critical alerts (only for new events, not duplicates)
            if topic_category == "event":
                self.detect_critical_alerts(device_serial, topic, data)

            conn.commit()
            logger.info(f"Data processed for device {device_serial} from topic {topic} with timestamp {parsed_timestamp}")

        except psycopg2.IntegrityError as e:
            if "duplicate key value violates unique constraint" in str(e):
                logger.debug(f"Duplicate record skipped for device {device_serial} on topic {topic}")
                # Don't send alert email for duplicates
                conn.rollback()
            else:
                logger.error(f"Database integrity error for device {device_serial}: {e}")
                conn.rollback()
                self.send_alert_email(
                    "Database Integrity Error",
                    f"Database integrity error for device {device_serial} on topic {topic}: {e}",
                    "critical"
                )
        except Exception as e:
            logger.error(f"Database error for device {device_serial}: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to insert data for device {device_serial} on topic {topic}: {e}",
                "critical"
            )
        finally:
            cur.close()
            conn.close()

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        """MQTT connection callback"""
        if reason_code == 0:
            logger.info(f"Connected to MQTT broker at {MQTT_BROKER}:{MQTT_PORT} with client ID {MQTT_CLIENT_ID}")
            self.last_connection_time = datetime.now(timezone.utc)
            self.reconnect_count = 0
            
            for topic, qos in MQTT_TOPICS:
                client.subscribe(topic, qos)
                logger.info(f"Subscribed to {topic} with QoS {qos}")
            
            if self.reconnect_count > 0:
                self.send_alert_email("MQTT Connection Restored", f"Successfully reconnected to MQTT broker after {self.reconnect_count} attempts")
        else:
            logger.error(f"Connection failed with reason code {reason_code}")
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
        logger.warning(f"Disconnected from MQTT broker with reason code {reason_code}")
        if self.running:
            self.reconnect_count += 1
            self.send_alert_email("MQTT Disconnection", f"Disconnected from MQTT broker. Reason code: {reason_code}. Reconnect attempt: {self.reconnect_count}", "warning")

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
        
        # Track connection state
        connected = False
        while self.running and not connected:
            connected = self.connect_with_retry()
            if not connected:
                logger.error("Failed to connect to MQTT broker. Retrying in 30 seconds...")
                time.sleep(30)
        
        if not connected:
            logger.error("Failed to connect to MQTT broker after all retries")
            return False
        
        try:
            # Start the MQTT loop in a separate thread
            self.client.loop_start()
            
            # Main service loop
            while self.running:
                if not self.client.is_connected():
                    logger.warning("MQTT connection lost, attempting to reconnect...")
                    self.client.loop_stop()
                    connected = False
                    while self.running and not connected:
                        connected = self.connect_with_retry()
                        if not connected:
                            logger.error("Failed to reconnect, retrying in 30 seconds...")
                            time.sleep(30)
                    if connected:
                        self.client.loop_start()
                
                # Sleep to reduce CPU usage
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
        finally:
            self.client.loop_stop()
            self.client.disconnect()
        
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
