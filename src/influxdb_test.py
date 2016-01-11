import csv
import time

# Must install from pip
import nanotime
from requests_futures.sessions import FuturesSession

# InfluxDB server details
INFLUX_URL = 'http://localhost'
INFLUX_PORT = '8086'
INFLUX_DB_NAME = 'stream'

# How many points to batch for each post
BATCH_AMOUNT = 2000

# Simulate stream id, change for each parallel process you run
STREAM_ID = '01'

# Pick from the data directory
FILE_NAME = 'mgh001.csv'

session = FuturesSession()
t = nanotime.now().milliseconds()


def bg_cb(sess, resp):
    # Print response and round-trip time for POST operation
    print("Response: " + str(resp.status_code) + ", Delta: " + str(nanotime.now().milliseconds() - t))

with open('../data/' + FILE_NAME, 'rt') as f:
    reader = csv.reader(f)
    count = 0
    s = ""
    for row in reader:
        count += 1

        # Parse ECG measurements
        lead1 = row[1]
        lead2 = row[2]

        # Buffer lines for influxdb
        s += 'ecg,stream_id=' + STREAM_ID + ' lead1=' + str(lead1) + ",lead2=" + str(lead2) + " " + str(nanotime.now().nanoseconds()) + "\n"

        # Simulate delay for each measurement
        time.sleep(0.003)

        # POST to influxdb when batch threshold is reached
        if count > BATCH_AMOUNT:

            t = nanotime.now().milliseconds()

            # POST operation is async so it wont delay the timestamping
            future = session.post(INFLUX_URL + ':' + INFLUX_PORT + '/write?db=' + INFLUX_DB_NAME, data=s, background_callback=bg_cb)

            s = ''
            count = 0
