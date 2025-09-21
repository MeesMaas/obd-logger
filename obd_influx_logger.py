import os
import time
from dotenv import load_dotenv
import obd
from influxdb_client import InfluxDBClient, Point, WriteOptions
import serial
import adafruit_gps

# Load environment variables from .env
load_dotenv()

INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# Only query RPM, MAF (Mass Air Flow), and SPEED
OBD_COMMANDS = {
    "rpm": obd.commands.RPM,
    "maf": obd.commands.MAF,
    "speed": obd.commands.SPEED,
}

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", 1.0))  # seconds

# Setup serial interface for GPS module
uart = serial.Serial("/dev/ttyUSB1", baudrate=9600, timeout=10)

# Create a GPS module instance
gps = adafruit_gps.GPS(uart, debug=False)

# Configure GPS output and update rate
gps.send_command(b"PMTK314,0,1,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0")
gps.send_command(b"PMTK220,1000")

def get_obd_data(connection):
    data = {}
    for key, cmd in OBD_COMMANDS.items():
        response = connection.query(cmd)
        if not response.is_null():
            data[key] = response.value.magnitude
    return data

def get_gps_data(last_print):
    gps.update()
    current = time.monotonic()
    if current - last_print >= POLL_INTERVAL:
        last_print = current
        if not gps.has_fix:
            print("Waiting for fix...")

        latitude = gps.latitude
        longitude = gps.longitude
        altitude = gps.altitude_m if gps.altitude_m is not None else float('nan')
        heading = gps.track_angle_deg
        return (latitude, longitude, altitude, heading), last_print
    return None, last_print

def main():
    connection = obd.OBD()
    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG
    )
    write_api = client.write_api(write_options=WriteOptions(batch_size=1))

    last_print = time.monotonic()

    try:
        while True:
            data = get_obd_data(connection)
            gps_data, last_print = get_gps_data(last_print)
            if gps_data:
                latitude, longitude, altitude, heading = gps_data
                data.update({
                    "latitude": latitude,
                    "longitude": longitude,
                    "altitude": altitude,
                    "heading": heading
                })

            if data:
                print(f"OBD Data: {data}")
                point = Point("obd_data")
                for field, value in data.items():
                    point = point.field(field, value)
                write_api.write(bucket=INFLUXDB_BUCKET, record=point)
            else:
                print("No OBD data found.")
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("Stopping OBD logger.")
    finally:
        write_api.__del__()
        client.__del__()
        connection.close()

if __name__ == "__main__":
    main()