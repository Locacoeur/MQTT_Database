import paho.mqtt.client as mqtt
import json
import uuid
import psycopg2
from psycopg2 import pool
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
from decouple import config
from cachetools import TTLCache

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

# Timestamp debug logger
timestamp_logger = logging.getLogger('timestamp_debug')
timestamp_handler = logging.FileHandler('timestamp_debug.log')
timestamp_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
timestamp_logger.addHandler(timestamp_handler)
timestamp_logger.setLevel(logging.DEBUG)

# MQTT settings
MQTT_BROKER = config("MQTT_BROKER", default="mqtt.locacoeur.com")
MQTT_PORT = int(config("MQTT_PORT", default=8883))
MQTT_TOPICS = [("LC1/+/event/#", 0), ("LC1/+/command/#", 0), ("LC1/+/result", 0)]
MQTT_CLIENT_ID = f"locacoeur-client-{uuid.uuid4()}"
MQTT_CA_CERT = config("MQTT_CA_CERT", default="certs/ca.crt")
MQTT_CLIENT_CERT = config("MQTT_CLIENT_CERT", default="certs/client.crt")
MQTT_CLIENT_KEY = config("MQTT_CLIENT_KEY", default="certs/client.key")
MQTT_USERNAME = config("MQTT_USERNAME", default="locacoeur")
MQTT_PASSWORD = config("MQTT_PASSWORD", default=None)

# Database settings
DB_CONFIG = {
    "dbname": config("DB_NAME", default="mqtt_db"),
    "user": config("DB_USER", default="mqtt_user"),
    "password": config("DB_PASSWORD"),
    "host": config("DB_HOST", default="91.134.90.10"),
    "port": config("DB_PORT", default="5432")
}

# Email settings
EMAIL_CONFIG = {
    "smtp_server": config("SMTP_SERVER", default="ssl0.ovh.net"),
    "smtp_port": int(config("SMTP_PORT", default=587)),
    "username": config("SMTP_USERNAME", default="support@locacoeur.com"),
    "password": config("SMTP_PASSWORD"),
    "from_email": config("SMTP_FROM_EMAIL", default="support@locacoeur.com"),
    "to_emails": config("SMTP_TO_EMAILS", default="alertHousse@proton.me").split(","),
    "enabled": config("SMTP_ENABLED", default=True, cast=bool)
}

class MQTTService:
    def __init__(self):
        self.running = True
        self.reconnect_count = 0
        self.last_connection_time = None
        self.email_cache = TTLCache(maxsize=100, ttl=3600)  # 1-hour email rate limit
        self.alert_cache = TTLCache(maxsize=100, ttl=60)    # 60s for alert follow-ups
        self.db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **DB_CONFIG)
        self.setup_signal_handlers()
        self.initialize_db()
        self.setup_mqtt_client()

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
        self.db_pool.closeall()
        sys.exit(0)

    def connect_db(self) -> Optional[psycopg2.extensions.connection]:
        """Get a database connection from the pool"""
        try:
            return self.db_pool.getconn()
        except Exception as e:
            logger.error(f"Failed to get database connection: {e}")
            self.send_alert_email("Database Connection Failed", f"Failed to get database connection: {e}", "critical")
            return None

    def release_db(self, conn):
        """Release a database connection back to the pool"""
        try:
            self.db_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Failed to release database connection: {e}")

    logger = logging.getLogger(__name__)

    def get_server_id(self) -> int:
        """Retrieve or create server_id for the current server"""
        conn = self.connect_db()
        if not conn:
            logger.error("Failed to connect to database for server_id retrieval")
            return None
        try:
            cur = conn.cursor()
            # Check for server with environment='production' and mqtt_url
            mqtt_url = "mqtts://mqtt.locacoeur.com:8883"
            cur.execute(
                """
                SELECT server_id FROM Servers
                WHERE environment = %s AND mqtt_url = %s
                """,
                ("production", mqtt_url)
            )
            result = cur.fetchone()
            if result:
                return result[0]

            # Insert new server if not exists
            cur.execute(
                """
                INSERT INTO Servers (environment, mqtt_url)
                VALUES (%s, %s)
                ON CONFLICT (environment) DO NOTHING
                RETURNING server_id
                """,
                ("production", mqtt_url)
            )
            result = cur.fetchone()
            server_id = result[0] if result else None
            if server_id is None:
                # Fetch server_id if insert failed due to conflict
                cur.execute(
                    """
                    SELECT server_id FROM Servers
                    WHERE environment = %s AND mqtt_url = %s
                    """,
                    ("production", mqtt_url)
                )
                result = cur.fetchone()
                server_id = result[0] if result else None
            if server_id is None:
                raise ValueError("Failed to retrieve or create server_id")
            conn.commit()
            logger.info(f"Created or retrieved server_id {server_id} for environment='production'")
            return server_id
        except Exception as e:
            logger.error(f"Error retrieving or creating server_id: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to retrieve or create server_id: {e}",
                "critical"
            )
            return None
        finally:
            cur.close()
            self.release_db(conn)

    def initialize_db(self):
        """Initialize database schema"""
        conn = self.connect_db()
        if not conn:
            return
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Servers (
                    server_id SERIAL PRIMARY KEY,
                    environment TEXT NOT NULL,
                    mqtt_url TEXT NOT NULL,
                    UNIQUE (environment)
                );
                CREATE TABLE IF NOT EXISTS Devices (
                    serial VARCHAR(50) PRIMARY KEY,
                    mqtt_broker_url VARCHAR(255),
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS Commands (
                    command_id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
                    server_id INTEGER REFERENCES Servers(server_id) ON DELETE RESTRICT,
                    operation_id VARCHAR(50),
                    topic VARCHAR(255),
                    message_id VARCHAR(50),
                    payload JSONB,
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    is_get BOOLEAN
                );
                CREATE TABLE IF NOT EXISTS Results (
                    result_id SERIAL PRIMARY KEY,
                    command_id INTEGER REFERENCES Commands(command_id) ON DELETE SET NULL,
                    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
                    topic VARCHAR(255),
                    result_status VARCHAR(50),
                    result_message TEXT,
                    payload JSONB,
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS Events (
                    event_id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
                    server_id INTEGER REFERENCES Servers(server_id) ON DELETE RESTRICT,
                    operation_id VARCHAR(50),
                    topic VARCHAR(255),
                    message_id VARCHAR(50),
                    payload JSONB,
                    event_timestamp TIMESTAMP WITHOUT TIME ZONE,
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    original_timestamp TEXT,
                    UNIQUE (device_serial, topic, event_timestamp)
                );
                CREATE TABLE IF NOT EXISTS LEDs (
                    led_id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
                    led_type VARCHAR(50) NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    description TEXT,
                    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT leds_status_check CHECK (status IN ('Green', 'Red', 'Off')),
                    UNIQUE (device_serial, led_type)
                );
                CREATE TABLE IF NOT EXISTS device_data (
                    id SERIAL PRIMARY KEY,
                    device_serial VARCHAR(50) REFERENCES Devices(serial) ON DELETE CASCADE,
                    topic VARCHAR(255),
                    battery INTEGER CHECK (battery >= 0 AND battery <= 100),
                    connection VARCHAR(50),
                    defibrillator INTEGER CHECK (defibrillator >= 0),
                    latitude DOUBLE PRECISION CHECK (latitude BETWEEN -90 AND 90),
                    longitude DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
                    power_source VARCHAR(50),
                    timestamp BIGINT,
                    led_power VARCHAR(50) CHECK (led_power IN ('Green', 'Red')),
                    led_defibrillator VARCHAR(50) CHECK (led_defibrillator IN ('Green', 'Red')),
                    led_monitoring VARCHAR(50) CHECK (led_monitoring IN ('Green', 'Red')),
                    led_assistance VARCHAR(50) CHECK (led_assistance IN ('Green', 'Red')),
                    led_mqtt VARCHAR(50) CHECK (led_mqtt IN ('Green', 'Red')),
                    led_environmental VARCHAR(50) CHECK (led_environmental IN ('Green', 'Red', 'Off')),
                    payload JSONB,
                    received_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    alert_id INTEGER,
                    alert_message VARCHAR(255),
                    original_timestamp BIGINT
                );
            """)
            conn.commit()
            logger.info("Database schema initialized")
        except Exception as e:
            logger.error(f"Error initializing database schema: {e}")
            conn.rollback()
        finally:
            cur.close()
            self.release_db(conn)

    def backup_device_data(self):
        """Backup old device_data records and clean up"""
        conn = self.connect_db()
        if not conn:
            return
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO device_data_backup
                SELECT *, CURRENT_TIMESTAMP FROM device_data WHERE received_at < %s
            """, (datetime.now(timezone.utc) - timedelta(days=30),))
            cur.execute("DELETE FROM device_data WHERE received_at < %s", 
                    (datetime.now(timezone.utc) - timedelta(days=30),))
            conn.commit()
            logger.info("Backed up and cleaned old device_data")
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            conn.rollback()
        finally:
            cur.close()
            self.release_db(conn)

    def send_alert_email(self, subject: str, message: str, priority: str = "normal"):
        """Send email alert with rate limiting"""
        if not EMAIL_CONFIG["enabled"]:
            logger.debug(f"Email alerts disabled. Would send: {subject}")
            return
        
        cache_key = f"{subject}:{priority}:{message[:50]}"
        if cache_key in self.email_cache:
            logger.debug(f"Email suppressed for {subject} (rate limit)")
            return
        self.email_cache[cache_key] = True

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

    timestamp_logger = logging.getLogger("timestamp_debug")

    def parse_timestamp(self, timestamp_value: Any, device_serial: str, return_unix: bool = False) -> Any:
        """Parse timestamps and return UTC datetime or Unix timestamp (milliseconds)"""
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
                    logger.warning(f"Device {device_serial}: Failed to parse timestamp string: {timestamp_value}")
        elif isinstance(timestamp_value, datetime):
            if timestamp_value.tzinfo is None:
                parsed_dt = timestamp_value.replace(tzinfo=timezone.utc)
            else:
                parsed_dt = timestamp_value.astimezone(timezone.utc)

        if parsed_dt is None:
            logger.warning(f"Device {device_serial}: Unable to parse timestamp: {timestamp_value}, using current time")
            return int(current_time.timestamp() * 1000) if return_unix else current_time

        min_valid_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        max_valid_time = current_time + timedelta(days=1)
        if parsed_dt < min_valid_time or parsed_dt > max_valid_time:
            logger.warning(f"Device {device_serial}: Timestamp out of range ({parsed_dt}), using current time")
            timestamp_logger.debug(f"Device {device_serial}: Replaced out-of-range timestamp {timestamp_value} -> {current_time}")
            return int(current_time.timestamp() * 1000) if return_unix else current_time

        timestamp_logger.debug(f"Device {device_serial}: Parsed timestamp {timestamp_value} -> {parsed_dt}")
        return int(parsed_dt.timestamp() * 1000) if return_unix else parsed_dt

    def detect_critical_alerts(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Detect critical conditions and send alerts"""
        alerts = []
        critical_alert_messages = ["Defibrillator fault", "Power is cut", "Device is removed"]

        if "alert" in topic.lower():
            alert_message = data.get("message")
            if alert_message in critical_alert_messages:
                alerts.append(f"Critical alert: {alert_message} (ID: {data.get('id')})")
                self.alert_cache[device_serial] = data  # Store alert for follow-up

        battery = data.get("battery")
        if battery is not None and isinstance(battery, (int, float)) and battery < 20:
            alerts.append(f"Low battery: {battery}%")

        led_mqtt = data.get("led_mqtt")
        if led_mqtt == "Red":
            alerts.append("Device connection lost")

        led_defibrillator = data.get("led_defibrillator")
        if led_defibrillator == "Red":
            alerts.append("Defibrillator not ready")

        led_power = data.get("led_power")
        if led_power == "Red":
            alerts.append("Power supply issue")

        led_environmental = data.get("led_environmental")
        if led_environmental == "Red":
            alerts.append("Temperature outside safe limits (below 5°C or above 40°C)")
        elif led_environmental == "Off":
            logger.debug(f"Device {device_serial}: Normal temperature range")

        if alerts:
            alert_message = f"Critical alerts for device {device_serial}:\n" + "\n".join(f"- {alert}" for alert in alerts)
            alert_message += f"\n\nTopic: {topic}\nData: {json.dumps(data, indent=2)}"
            self.send_alert_email(f"Critical Alert - Device {device_serial}", alert_message, "critical")
            logger.warning(f"Critical alerts sent for device {device_serial}: {alerts}")
        else:
            logger.debug(f"No critical alerts detected for device {device_serial} on topic {topic}")

    def insert_command(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert command data into the Commands table"""
        conn = self.connect_db()
        if not conn:
            return
        try:
            cur = conn.cursor()
            payload_str = json.dumps(data, sort_keys=True)
            server_id = self.get_server_id()
            if server_id is None:
                logger.error(f"No server_id found for device {device_serial}")
                self.send_alert_email(
                    "Database Error",
                    f"No server_id found for device {device_serial} on topic {topic}",
                    "critical"
                )
                return

            operation_id = topic.split("/")[-1]
            if not operation_id:
                logger.error(f"Invalid operation_id for topic {topic}")
                self.send_alert_email(
                    "Database Error",
                    f"Invalid operation_id for device {device_serial} on topic {topic}",
                    "critical"
                )
                return

            message_id = data.get("message_id", str(uuid.uuid4()))

            cur.execute(
                """
                INSERT INTO Commands (
                    device_serial, server_id, operation_id, topic, message_id,
                    payload, created_at, is_get
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    device_serial[:50],
                    server_id,
                    operation_id[:50],
                    topic[:255],
                    message_id[:50],
                    payload_str,
                    datetime.now(),
                    "get" in operation_id.lower()
                )
            )
            conn.commit()
            logger.info(f"Inserted command for device {device_serial} on topic {topic}")
        except Exception as e:
            logger.error(f"Database error for device {device_serial}: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to insert command for device {device_serial} on topic {topic}: {e}",
                "critical"
            )
        finally:
            cur.close()
            self.release_db(conn)
    

    logger = logging.getLogger(__name__)

    def insert_device_data(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert device data into the device_data table"""
        conn = self.connect_db()
        if not conn:
            logger.error("Failed to connect to database")
            return
        try:
            cur = conn.cursor()
            payload_str = json.dumps(data, sort_keys=True)
            timestamp = self.parse_timestamp(data.get("timestamp"), device_serial, return_unix=True)
            if timestamp is None:
                logger.error(f"Invalid timestamp for device {device_serial} on topic {topic}")
                self.send_alert_email(
                    "Invalid Timestamp",
                    f"Invalid timestamp for device {device_serial} on topic {topic}: {data.get('timestamp')}",
                    "warning"
                )
                return

            # Insert into Devices if not exists
            logger.debug(f"Inserting device {device_serial} into Devices table")
            cur.execute(
                """
                INSERT INTO Devices (serial, mqtt_broker_url)
                VALUES (%s, %s)
                ON CONFLICT (serial) DO NOTHING
                """,
                (device_serial[:50], "mqtts://mqtt.locacoeur.com:8883")
            )

            # Handle different event types
            if topic.endswith("/event/location"):
                latitude = data.get("latitude")
                longitude = data.get("longitude")
                logger.debug(f"Processing location event for {device_serial}: latitude={latitude}, longitude={longitude}")
                if not isinstance(latitude, (int, float)) or not isinstance(longitude, (int, float)):
                    logger.error(f"Invalid location data types for device {device_serial}: latitude={latitude}, longitude={longitude}")
                    self.send_alert_email(
                        "Invalid Location Data",
                        f"Invalid location data types for device {device_serial}: latitude={latitude}, longitude={longitude}",
                        "warning"
                    )
                    return
                if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
                    logger.error(f"Invalid location data for device {device_serial}: latitude={latitude}, longitude={longitude}")
                    self.send_alert_email(
                        "Invalid Location Data",
                        f"Invalid location for device {device_serial}: latitude={latitude}, longitude={longitude}",
                        "warning"
                    )
                    return
                cur.execute(
                    """
                    INSERT INTO device_data (
                        device_serial, topic, latitude, longitude, timestamp,
                        original_timestamp, received_at, payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        device_serial[:50],
                        topic[:255],
                        float(latitude),
                        float(longitude),
                        timestamp,
                        timestamp,
                        datetime.now(),
                        payload_str
                    )
                )
            elif topic.endswith("/event/status"):
                battery = data.get("battery")
                led_power = data.get("led_power")
                led_defibrillator = data.get("led_defibrillator")
                led_monitoring = data.get("led_monitoring")
                led_assistance = data.get("led_assistance")
                led_mqtt = data.get("led_mqtt")
                led_environmental = data.get("led_environmental")
                logger.debug(f"Processing status event for {device_serial}: battery={battery}, "
                            f"led_power={led_power}, led_defibrillator={led_defibrillator}, "
                            f"led_monitoring={led_monitoring}, led_assistance={led_assistance}, "
                            f"led_mqtt={led_mqtt}, led_environmental={led_environmental}")
                if battery is not None and (not isinstance(battery, int) or not 0 <= battery <= 100):
                    logger.error(f"Invalid battery value for device {device_serial}: {battery}")
                    self.send_alert_email(
                        "Invalid Status Data",
                        f"Invalid battery value for device {device_serial}: {battery}",
                        "warning"
                    )
                    return
                valid_leds = {"Green", "Red", "Off"}
                for led, value in [
                    ("Power", led_power),
                    ("Defibrillator", led_defibrillator),
                    ("Monitoring", led_monitoring),
                    ("Assistance", led_assistance),
                    ("MQTT", led_mqtt),
                    ("Environmental", led_environmental)
                ]:
                    if value is not None and value not in valid_leds:
                        logger.error(f"Invalid LED value for {led} on device {device_serial}: {value}")
                        self.send_alert_email(
                            "Invalid Status Data",
                            f"Invalid LED value for {led} on device {device_serial}: {value}",
                            "warning"
                        )
                        return
                logger.debug(f"Inserting into device_data for {device_serial}: battery={battery}, "
                            f"led_power={led_power}, led_defibrillator={led_defibrillator}, "
                            f"led_monitoring={led_monitoring}, led_assistance={led_assistance}, "
                            f"led_mqtt={led_mqtt}, led_environmental={led_environmental}")
                
                # FIXED: Added the missing 13th placeholder (%s) to match 13 parameters
                cur.execute(
                    """
                    INSERT INTO device_data (
                        device_serial, topic, battery, led_power, led_defibrillator,
                        led_monitoring, led_assistance, led_mqtt, led_environmental,
                        timestamp, original_timestamp, received_at, payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        device_serial[:50],
                        topic[:255],
                        battery,
                        led_power if led_power else None,
                        led_defibrillator if led_defibrillator else None,
                        led_monitoring if led_monitoring else None,
                        led_assistance if led_assistance else None,
                        led_mqtt if led_mqtt else None,
                        led_environmental if led_environmental else None,
                        timestamp,
                        timestamp,
                        datetime.now(),
                        payload_str
                    )
                )
                
                # Update LEDs table for non-None values
                led_values = [
                    ("Power", led_power),
                    ("Defibrillator", led_defibrillator),
                    ("Monitoring", led_monitoring),
                    ("Assistance", led_assistance),
                    ("MQTT", led_mqtt),
                    ("Environmental", led_environmental)
                ]
                for led_type, status in led_values:
                    if status is not None:
                        logger.debug(f"Preparing LED insert: device={device_serial}, type={led_type}, status={status}")
                        cur.execute(
                            """
                            INSERT INTO LEDs (device_serial, led_type, status, description, last_updated)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (device_serial, led_type)
                            DO UPDATE SET status = EXCLUDED.status, description = EXCLUDED.description, last_updated = EXCLUDED.last_updated
                            """,
                            (device_serial[:50], led_type, status, None, datetime.now())
                        )
                        logger.debug(f"Inserted LED: device={device_serial}, type={led_type}, status={status}")
                
                # Check for critical conditions
                if battery is not None and battery < 20:
                    self.send_alert_email(
                        "Low Battery",
                        f"Low battery for device {device_serial}: {battery}%",
                        "critical"
                    )
                if led_power == "Red":
                    self.send_alert_email(
                        "Power Supply Issue",
                        f"Power supply issue for device {device_serial}: led_power=Red",
                        "critical"
                    )
            elif topic.endswith("/event/alert"):
                alert_id = data.get("id")
                alert_message = data.get("message")
                logger.debug(f"Processing alert event for {device_serial}: alert_id={alert_id}, alert_message={alert_message}")
                if not isinstance(alert_id, int):
                    logger.error(f"Invalid alert_id for device {device_serial}: {alert_id}")
                    self.send_alert_email(
                        "Invalid Alert Data",
                        f"Invalid alert_id for device {device_serial}: {alert_id}",
                        "warning"
                    )
                    return
                cur.execute(
                    """
                    INSERT INTO device_data (
                        device_serial, topic, alert_id, alert_message, timestamp,
                        original_timestamp, received_at, payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        device_serial[:50],
                        topic[:255],
                        alert_id,
                        alert_message[:255] if alert_message else None,
                        timestamp,
                        timestamp,
                        datetime.now(),
                        payload_str
                    )
                )
                self.send_alert_email(
                    f"Critical Alert - Device {device_serial}",
                    f"Alert for device {device_serial}: {alert_message}",
                    "critical"
                )
            
            conn.commit()
            logger.info(f"Inserted into device_data for device {device_serial} on topic {topic}")
            
        except Exception as e:
            logger.error(f"Database error for device {device_serial}: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to insert device data for device {device_serial} on topic {topic}: {e}",
                "critical"
            )
            return
        finally:
            cur.close()
            self.release_db(conn)
        
    def insert_version_data(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert version data into the Events table"""
        conn = self.connect_db()
        if not conn:
            logger.error("Failed to connect to database")
            return
        try:
            cur = conn.cursor()
            server_id = self.get_server_id()
            if server_id is None:
                logger.error(f"Failed to get server_id for device {device_serial}")
                return
            payload_str = json.dumps(data, sort_keys=True)
            timestamp = self.parse_timestamp(data.get("timestamp"), device_serial, return_unix=False)
            if timestamp is None:
                logger.error(f"Invalid timestamp for device {device_serial} on topic {topic}")
                self.send_alert_email(
                    "Invalid Timestamp",
                    f"Invalid timestamp for device {device_serial} on topic {topic}: {data.get('timestamp')}",
                    "warning"
                )
                return
            version = data.get("version")
            if not isinstance(version, str):
                logger.error(f"Invalid version for device {device_serial}: {version}")
                self.send_alert_email(
                    "Invalid Version Data",
                    f"Invalid version for device {device_serial}: {version}",
                    "warning"
                )
                return
            logger.debug(f"Inserting version event for {device_serial}: version={version}")
            cur.execute(
                """
                INSERT INTO Events (
                    device_serial, server_id, operation_id, topic, payload,
                    event_timestamp, created_at, original_timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (device_serial, topic, event_timestamp)
                DO UPDATE SET
                    payload = EXCLUDED.payload,
                    created_at = EXCLUDED.created_at,
                    original_timestamp = EXCLUDED.original_timestamp
                """,
                (
                    device_serial[:50],
                    server_id,
                    "version",
                    topic[:255],
                    payload_str,
                    timestamp,
                    datetime.now(),
                    data.get("timestamp")
                )
            )
            conn.commit()
            logger.info(f"Inserted version data for device {device_serial} on topic {topic}")
        except Exception as e:
            logger.error(f"Database error for device {device_serial}: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to insert version data for device {device_serial} on topic {topic}: {e}",
                "critical"
            )
            return
        finally:
            cur.close()
            self.release_db(conn)

    def insert_log_data(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert log data into the Events table"""
        conn = self.connect_db()
        if not conn:
            logger.error("Failed to connect to database")
            return
        try:
            cur = conn.cursor()
            server_id = self.get_server_id()
            if server_id is None:
                logger.error(f"Failed to get server_id for device {device_serial}")
                return
            payload_str = json.dumps(data, sort_keys=True)
            timestamp = self.parse_timestamp(data.get("timestamp"), device_serial, return_unix=False)
            if timestamp is None:
                logger.error(f"Invalid timestamp for device {device_serial} on topic {topic}")
                self.send_alert_email(
                    "Invalid Timestamp",
                    f"Invalid timestamp for device {device_serial} on topic {topic}: {data.get('timestamp')}",
                    "warning"
                )
                return
            log_message = data.get("log_message")
            if not isinstance(log_message, str):
                logger.error(f"Invalid log_message for device {device_serial}: {log_message}")
                self.send_alert_email(
                    "Invalid Log Data",
                    f"Invalid log_message for device {device_serial}: {log_message}",
                    "warning"
                )
                return
            logger.debug(f"Inserting log event for {device_serial}: log_message={log_message}")
            cur.execute(
                """
                INSERT INTO Events (
                    device_serial, server_id, operation_id, topic, payload,
                    event_timestamp, created_at, original_timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (device_serial, topic, event_timestamp)
                DO UPDATE SET
                    payload = EXCLUDED.payload,
                    created_at = EXCLUDED.created_at,
                    original_timestamp = EXCLUDED.original_timestamp
                """,
                (
                    device_serial[:50],
                    server_id,
                    "log",
                    topic[:255],
                    payload_str,
                    timestamp,
                    datetime.now(),
                    data.get("timestamp")
                )
            )
            conn.commit()
            logger.info(f"Inserted log data for device {device_serial} on topic {topic}")
        except Exception as e:
            logger.error(f"Database error for device {device_serial}: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to insert log data for device {device_serial} on topic {topic}: {e}",
                "critical"
            )
            return
        finally:
            cur.close()
            self.release_db(conn)

    def insert_result(self, device_serial: str, topic: str, data: Dict[str, Any]) -> None:
        """Insert result data into the Results table"""
        conn = self.connect_db()
        if not conn:
            return
        try:
            cur = conn.cursor()
            payload_str = json.dumps(data, sort_keys=True)
            result_status = data.get("status", "")[:50]  # Truncate to 50 chars
            result_message = data.get("message", "")
            timestamp = self.parse_timestamp(data.get("timestamp"), device_serial)

            if timestamp is None:
                logger.error(f"Invalid timestamp for device {device_serial} on topic {topic}")
                self.send_alert_email(
                    "Invalid Timestamp",
                    f"Invalid timestamp for device {device_serial} on topic {topic}: {data.get('timestamp')}",
                    "warning"
                )
                return

            # Insert into Devices if not exists
            cur.execute(
                """
                INSERT INTO Devices (serial, mqtt_broker_url)
                VALUES (%s, %s)
                ON CONFLICT (serial) DO NOTHING
                """,
                (device_serial[:50], "mqtts://mqtt.locacoeur.com:8883")
            )

            # Find the corresponding command_id (if any)
            command_id = None
            if "command_id" in data:
                cur.execute(
                    """
                    SELECT command_id FROM Commands
                    WHERE device_serial = %s AND command_id = %s
                    """,
                    (device_serial[:50], data["command_id"])
                )
                result = cur.fetchone()
                command_id = result[0] if result else None

            # Insert into Results
            cur.execute(
                """
                INSERT INTO Results (
                    command_id, device_serial, topic, result_status, result_message,
                    payload, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    command_id,
                    device_serial[:50],
                    topic[:255],
                    result_status,
                    result_message,
                    payload_str,
                    datetime.now()  # TIMESTAMP WITHOUT TIME ZONE
                )
            )
            conn.commit()
            logger.info(f"Inserted result for device {device_serial} on topic {topic}")
        except Exception as e:
            logger.error(f"Database error for device {device_serial}: {e}")
            conn.rollback()
            self.send_alert_email(
                "Database Error",
                f"Failed to insert result for device {device_serial} on topic {topic}: {e}",
                "critical"
            )
        finally:
            cur.close()
            self.release_db(conn)

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
            payload_str = json.dumps(data, sort_keys=True)

            # Validate payload based on operation
            if topic_category == "event":
                if operation_id == "location" and not all(key in data for key in ["latitude", "longitude"]):
                    logger.error(f"Invalid location payload for {device_serial}: {data}")
                    self.send_alert_email(
                        "Invalid Payload",
                        f"Location event missing latitude/longitude for {device_serial}: {data}",
                        "warning"
                    )
                    return
                elif operation_id == "version" and "firmware_version" not in data:
                    logger.error(f"Invalid version payload for {device_serial}: {data}")
                    self.send_alert_email(
                        "Invalid Payload",
                        f"Version event missing firmware_version for {device_serial}: {data}",
                        "warning"
                    )
                    return
                elif operation_id == "status" and not any(key in data for key in ["led_power", "led_defibrillator", "led_monitoring", "led_assistance", "led_mqtt", "led_environmental"]):
                    logger.error(f"Invalid status payload for {device_serial}: {data}")
                    self.send_alert_email(
                        "Invalid Payload",
                        f"Status event missing LED data for {device_serial}: {data}",
                        "warning"
                    )
                    return
                elif operation_id == "alert" and "message" not in data:
                    logger.error(f"Invalid alert payload for {device_serial}: {data}")
                    self.send_alert_email(
                        "Invalid Payload",
                        f"Alert event missing message for {device_serial}: {data}",
                        "warning"
                    )
                    return

            # Handle alert follow-ups
            if topic_category == "event" and operation_id in ["status", "location"]:
                if device_serial in self.alert_cache:
                    logger.info(f"Processing follow-up {operation_id} for alert on {device_serial}")
                    alert_data = self.alert_cache[device_serial]
                    self.send_alert_email(
                        f"Alert Follow-Up - Device {device_serial}",
                        f"Received {operation_id} event following alert: {json.dumps(alert_data, indent=2)}\nCurrent: {json.dumps(data, indent=2)}",
                        "warning"
                    )

            # Get server_id
            cur.execute("SELECT server_id FROM Servers WHERE environment = %s", ("production",))
            result = cur.fetchone()
            server_id = result[0] if result else 1

            # Ensure device exists
            cur.execute(
                """
                INSERT INTO Devices (serial, mqtt_broker_url)
                VALUES (%s, %s)
                ON CONFLICT (serial) DO NOTHING
                """,
                (device_serial, "mqtts://mqtt.locacoeur.com:8883")
            )

            # Handle Events
            if topic_category == "event":
                cur.execute(
                    """
                    INSERT INTO Events (
                        device_serial, server_id, operation_id, topic, message_id, 
                        payload, event_timestamp, created_at, original_timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (device_serial, topic, event_timestamp) DO UPDATE SET
                        created_at = EXCLUDED.created_at,
                        original_timestamp = EXCLUDED.original_timestamp
                    """,
                    (
                        device_serial,
                        server_id,
                        operation_id,
                        topic,
                        operation_id,
                        payload_str,
                        parsed_timestamp,
                        datetime.now(timezone.utc),
                        str(data.get("timestamp"))
                    )
                )

                # Update LEDs
                led_updates = {}
                alert_id = data.get("id") if operation_id == "Alert" else None
                if alert_id == 2:
                    led_updates["Power"] = ("Red", "Device is running on battery power")
                elif alert_id == 3:
                    led_updates["Defibrillator"] = ("Red", "Indicates a fault in the defibrillator")
                elif operation_id == "Status":
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

            # Handle Commands
            elif topic_category == "command":
                is_get = len(topic_parts) > 4 and topic_parts[4] == "get"
                cur.execute(
                    """
                    INSERT INTO Commands (
                        device_serial, server_id, operation_id, topic, message_id, payload, created_at, is_get
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        device_serial,
                        server_id,
                        operation_id,
                        topic,
                        operation_id,
                        payload_str,
                        datetime.now(timezone.utc),
                        is_get
                    )
                )

            # Handle Results
            elif topic_category == "result":
                result_status = data.get("status", "Success")
                result_message = data.get("message")
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

            # Check critical alerts
            if topic_category == "event":
                self.detect_critical_alerts(device_serial, topic, data)
                self.insert_device_data(device_serial, topic, data)

            conn.commit()
            logger.info(f"Data processed for device {device_serial} from topic {topic}")
        except psycopg2.errors.UniqueViolation as e:
            logger.debug(f"Duplicate record skipped for device {device_serial} on topic {topic}")
            conn.rollback()
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
            self.release_db(conn)

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
        """Handle incoming MQTT messages"""
        try:
            topic = msg.topic
            payload = json.loads(msg.payload.decode("utf-8"))
            device_serial = topic.split("/")[1]
            operation = topic.split("/")[2]

            if operation == "event":
                event_type = topic.split("/")[-1]
                if event_type in ["status", "location", "alert"]:
                    self.insert_device_data(device_serial, topic, payload)
                elif event_type == "version":
                    self.insert_version_data(device_serial, topic, payload)
                elif event_type == "log":
                    self.insert_log_data(device_serial, topic, payload)
            elif operation == "command":
                self.insert_command(device_serial, topic, payload)
            elif operation == "result":
                self.insert_result(device_serial, topic, payload)
            logger.info(f"Data processed for device {device_serial} from topic {topic}")
        except Exception as e:
            logger.error(f"Error processing message on topic {topic}: {e}")
            self.send_alert_email(
                "Message Processing Error",
                f"Failed to process message for device {device_serial} on topic {topic}: {e}",
                "critical"
            )
            return

    def on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        """MQTT disconnect callback"""
        logger.warning(f"Disconnected from MQTT broker with reason code {reason_code}")
        if self.running:
            self.reconnect_count += 1
            self.send_alert_email("MQTT Disconnection", f"Disconnected from MQTT broker. Reason code: {reason_code}. Reconnect attempt: {self.reconnect_count}", "warning")

    def on_log(self, client, userdata, level, buf):
        """MQTT log callback"""
        logger.debug(f"MQTT log: {buf}")

    def connect_with_retry(self, max_retries=10, retry_delay=5, max_total_attempts=50):
        """Connect to MQTT broker with retry logic"""
        total_attempts = 0
        while self.running and total_attempts < max_total_attempts:
            for attempt in range(max_retries):
                try:
                    logger.info(f"Attempting to connect to MQTT broker (attempt {attempt + 1}/{max_retries})")
                    self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
                    return True
                except Exception as e:
                    total_attempts += 1
                    logger.error(f"MQTT connection attempt {attempt + 1} failed: {e}")
                    if total_attempts >= max_total_attempts:
                        logger.error("Max total attempts reached")
                        self.send_alert_email("MQTT Connection Failed", "Max total connection attempts reached", "critical")
                        return False
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay = min(retry_delay * 1.5, 60)
            time.sleep(30)
        return False

    def setup_mqtt_client(self):
        """Setup MQTT client with TLS and callbacks"""
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=MQTT_CLIENT_ID,
            protocol=mqtt.MQTTv5,
            transport="tcp"
        )
        self.client.tls_set(
            ca_certs=MQTT_CA_CERT,
            certfile=MQTT_CLIENT_CERT,
            keyfile=MQTT_CLIENT_KEY,
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS
        )
        if MQTT_USERNAME and MQTT_PASSWORD:
            self.client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_log = self.on_log

    def run(self):
        """Main service loop"""
        logger.info("Starting LOCACOEUR MQTT service...")
        threading.Timer(86400, self.backup_device_data).start()  # Daily backup
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
            self.client.loop_start()
            while self.running:
                if not self.client.is_connected():
                    logger.warning("MQTT connection lost, attempting to reconnect...")
                    self.client.loop_stop()
                    connected = False
                    while self.running and not connected:
                        connected = self.connect_with_retry()
                        if not connected:
                            time.sleep(30)
                    if connected:
                        self.client.loop_start()
                time.sleep(1)
        finally:
            self.client.loop_stop()
            self.client.disconnect()
            self.db_pool.closeall()
        logger.info("LOCACOEUR MQTT service stopped")
        return True

if __name__ == "__main__":
    service = MQTTService()
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
