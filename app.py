import time

# mqtt
import json
# import influxdb

import psycopg2
#
# from influxdb import InfluxDBClient

from paho.mqtt import client as mqtt
import random

import datetime

from flask import Flask, render_template, request, Response, render_template_string, redirect, url_for


app = Flask(__name__)

host = ""
client_id = ""
time_sleep = ""
username = ""
password = ""
# broker = 'e2e7fce5-internet-facing-bef1dc639c9cdd63.elb.us-east-1.amazonaws.com'
port = 1883

# client = InfluxDBClient(host='localhost', port=8086)
# infc = InfluxDBClient(host='8.215.33.9', port=8086, username='admin', password='admin@123#')
# infc.create_database('drowdet')
# print(infc.get_list_database())
# infc.switch_database('drowdet')
#
conn = psycopg2.connect(
    host="IP Address PostgreSQL",
    database="Name DataBase",
    port=5432,
    user="UserName Postgre",
    password="Password postgre")

conn.autocommit = True
cursor = conn.cursor()
# # topic = "python/mqtt_docker/"
# # generate client ID with pub prefix randomly
# clientid = f'python-mqtt-{random.randint(0, 1000)}'
# un = 'python-mqtt'
# ps = 'python-mqtt'
# # username = ""
# # password = ""
# # host = ""
# # client_id = ""
# # time_sleep = 0
#
# # class DataStore():
# #     username = ""
# #     password = ""
# #     host = ""
# #     client_id = ""
# #     time_sleep = 0
# #
# # data = DataStore()
#
# def connect_mqtt():
#     def on_connect(client, userdata, flags, rc):
#         if rc == 0:
#             print("Connected to MQTT Broker!")
#         else:
#             print("Failed to connect, return code %d\n", rc)
#
#     client = mqtt.Client(clientid)
#     client.username_pw_set(username=un, password=ps)
#     client.on_connect = on_connect
#     client.connect(broker, port)
#     return client

def mqtt_auth(uname, upass, host, client_id):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
            # client.connect_flag = True
        else:
            print("Failed to connect, return code %d\n", rc)

    # mqtt.Client.connect_flag = False

    client = mqtt.Client(client_id)
    client.username_pw_set(username=uname, password=upass)
    client.on_connect = on_connect
    client.connect(host, port)
    return client

def publish_dd(client, msg):
    topics = "drowdet"
    # msg = "Mengantuk"
    result = client.publish(topics, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{topics}`")
    else:
        print(f"Failed to send message to topic {topics}")
    # return status


def subs_dd(client):
    topics = "drowdet/mqtt/"
    # msg = "Mengantuk"
    # result = client.publish(topics, msg)
    # result: [0, 1]
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{topics}` topic")

    client.subscribe(topics)
    client.on_message = on_message


@app.route('/')
def hello_world():
    return render_template('index.html')

@app.route('/login', methods = ['POST'])
def login():
    global host
    global client_id
    global time_sleep
    global username
    global password

    data = request.form

    host = data.get('broker')
    client_id = data.get('client')
    time_sleep = data.get('time')
    username = data.get('user')
    password = data.get('pass')

    # broker = host
    # port = 1883
    #
    # client_id = clid
    # broker = 'e2e7fce5-internet-facing-bef1dc639c9cdd63.elb.us-east-1.amazonaws.com'
    port = 1883

    # client_id = f'python-mqtt-{random.randint(0, 1000)}'

    def on_connect(client, userdata, flags, rc):

        if rc == 0:
            nonlocal status
            status = True
            print("Connected to MQTT Broker!")
            # return client.connected_flag
        else:
            print("Failed to connect, return code : ", rc)


    status = True
    client = mqtt.Client(client_id, clean_session=True)

    client.username_pw_set(username=username, password=password)
    client.on_connect = on_connect
    client.connect(host, port)
    client.loop_start()

    if not status :
        print(status)
        return render_template('failed.html')
    else:
        print(status)
        return render_template('streamval.html')

@app.route('/generate')
def generate():
    def genrandom():

        uname = username
        pword = password
        broke = host
        clied = client_id
        sleep = float(time_sleep)

        print(uname)
        print(pword)
        print(broke)
        print(clied)
        print(sleep)

        sleeptime = sleep/1000
        while True :
            client = mqtt_auth(uname,pword,broke,clied)

            x = datetime.datetime.now()

            year = str(x.year)
            month = str(x.month)
            day = str(x.day)
            hour = str(x.hour)
            min = str(x.minute)
            sec = str(x.second)
            ms = str(x.microsecond)

            t = (day, month, year)
            y = (hour, min, sec, ms)

            w = "/".join(t)
            z = ":".join(y)

            dt = (w, z)
            ts = " ".join(dt)

            element = datetime.datetime.strptime(ts, "%d/%m/%Y %H:%M:%S:%f")

            timestamp = datetime.datetime.timestamp(element)

            # {sleep: 0, speed: 50, acc: 20,
            # break: 0, loc: [20, 10], alt: 10, turn: 'L', steer: 30}

            loc = []
            loc.append(random.randint(10, 99))
            loc.append(random.randint(10, 99))

            steer = random.randint(-100, 100)

            if steer <= 0 :
                turn = 'L'
            else:
                turn = 'R'

            acc = random.randint(0, 80)
            sleep = random.randint(0, 1)
            speed = random.randint(0, 180)
            brake = random.randint(0,1)
            alt = random.randint(0, 100)

            json_msg = {}
            json_msg["timestamp"] = timestamp
            json_msg["sleep"] = sleep
            json_msg["speed"] = speed
            json_msg["acc"] = acc
            json_msg["brake"] = brake
            json_msg["loc"] = loc
            json_msg["alt"] = alt
            json_msg["turn"] = turn
            json_msg["steer"] = steer

            json_mess = json.dumps(json_msg)

            # json_body = [
            #     {
            #         "measurement": "random-generate-val",
            #         "tags": {
            #             "user": "Brilyan",
            #         },
            #         "time": datetime.datetime(x.year, x.day, x.month, x.hour, x.minute, x.second, x.microsecond),
            #         "fields": {
            #             "sleep": random.randint(0, 1),
            #             "speed": random.randint(0, 180),
            #             "acc": random.randint(0, 80),
            #             "steer": steer
            #         }
            #     }
            # ]
            time.sleep(sleeptime)

            postgres_insert_query = """ INSERT INTO acc (time_id, acc_val, sleep_val, speed_val, brake_val, alt_val) VALUES (%s,%s,%s,%s,%s,%s)"""
            record_to_insert = (timestamp, acc, sleep, speed, brake, alt)
            cursor.execute(postgres_insert_query, record_to_insert)

            conn.commit()

            publish_dd(client, json_mess)
            # infc.write_points(json_body)

            yield f"data:{json_mess}\n\n"

    return Response(genrandom(), mimetype='text/event-stream')

