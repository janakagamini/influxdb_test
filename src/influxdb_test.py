import csv
import time
import socket

# Must install from pip
import nanotime
from requests_futures.sessions import FuturesSession

# InfluxDB server details
INFLUX_URL = 'http://172.16.132.86'
INFLUX_PORT = '8086'
INFLUX_DB_NAME = 'ecg_stream_test'

# How many points to batch for each POST
BATCH_AMOUNT = 2000

# Simulate a unique stream_id (socket.gethostname() returns your computer name)
STREAM_ID = ''.join(e for e in socket.gethostname() if e.isalnum())

# Pick a file from the data directory
FILE_NAME = 'mgh001.csv'

session = FuturesSession()
t = nanotime.now().milliseconds()

# Called when POST completes
def bg_cb(sess, resp):
    # Print response and round-trip time for POST operation
    print("ID: " + STREAM_ID + ", Response: " + str(resp.status_code) + ", Delta: " + str(nanotime.now().milliseconds() - t))

with open('../data/' + FILE_NAME, 'rt') as f:
    reader = csv.reader(f)
    count = 0
    s = ''
    for row in reader:
        count += 1

        # Parse ECG measurements
        lead1 = row[1]
        lead2 = row[2]

        # Buffer lines for influxdb
        # E.g: ecg,stream_id=janakaOptiPlex9020 lead1=-0.576,lead2=0.012 1452486486726298112
        #
        # Measurement: ecg
        # Tags: stream_id
        # Values: lead1, lead2

        # Writing directly in nanoseconds
        #s += 'ecg,stream_id=' + STREAM_ID + ' lead1=' + str(lead1) + ",lead2=" + str(lead2) + " " + str(nanotime.now().nanoseconds()) + "\n"

        # Milliseconds converted to nanoseconds
        s += 'ecg,stream_id=' + STREAM_ID + ' lead1=' + str(lead1) + ",lead2=" + str(lead2) + " " + str(int(nanotime.now().milliseconds() * 1000000)) + "\n"

        # Sampling rate as per csv file
        time.sleep(0.003)

        # POST to influxdb when batch threshold is reached
        if count > BATCH_AMOUNT:

            t = nanotime.now().milliseconds()

            # POST operation is async so it wont delay the timestamping
            future = session.post(INFLUX_URL + ':' + INFLUX_PORT + '/write?db=' + INFLUX_DB_NAME, data=s, background_callback=bg_cb)

            s = ''
            count = 0
