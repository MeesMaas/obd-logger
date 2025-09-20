import os
import time
from dotenv import load_dotenv
import obd
from influxdb_client import InfluxDBClient, Point, WriteOptions

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

def get_obd_data(connection):
    data = {}
    for key, cmd in OBD_COMMANDS.items():
        response = connection.query(cmd)
        if not response.is_null():
            data[key] = response.value.magnitude
    return data

def main():
    connection = obd.OBD()
    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG
    )
    write_api = client.write_api(write_options=WriteOptions(batch_size=1))
    try:
        while True:
            data = get_obd_data(connection)
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