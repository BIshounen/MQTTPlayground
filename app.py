import paho.mqtt.client as paho
from paho import mqtt
from flask import Flask, request
import requests
import config
import creds


credentials_file = "creds.py"
MQTT_url = "079ac9dc74e24ba6b8f8f5f3bcd9dd10.s1.eu.hivemq.cloud"
MQTT_port = 8883
cert = "isrgrootx1.pem"

app = Flask(__name__)
client = paho.Client(paho.CallbackAPIVersion.VERSION2, client_id="tester2", userdata=None, protocol=paho.MQTTv5)
connected_to_hive = False


@app.route('/')
def index():
    return "hello world!"


@app.route('/send/', methods=['POST'])
def send():
    if not connected_to_hive:
        return 'no hive connection', 500
    if 'topic' in request.json:
        client.publish(topic=request.json['topic'], payload=str(request.json.get('payload', '')), qos=1)
        return request.json, 201
    else:
        return 'no topic provided', 422


def on_log(_client, _userdata, _level, _buf):
    print("log: ", _buf)


def connect_to_hivemq():

    client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS, ca_certs=cert)
    client.username_pw_set(creds.creds['username'], creds.creds['password'])
    client.on_log = on_log
    client.on_connect = on_connect
    client.connect(host=MQTT_url, port=MQTT_port, keepalive=60, clean_start=True)


def on_connect(_client, _userdata, _connect_flags, _reason_code, _properties):
    if _reason_code == 0:
        global connected_to_hive
        connected_to_hive = True
        print(connected_to_hive)


def on_message(_client, _user_data, _msg):
    body = creds.creds
    result = requests.post(config.NX_LOGIN_URL, data=body, verify=False)
    if result.status_code == 200:
        token = result.json()["token"]
    else:
        print(result.status_code)
        return

    body = {"state": "instant", "caption": "MQTT Message", "description": _msg.payload.decode('utf-8')}
    result = requests.post(config.NX_GENERIC_EVENT_URL,
                           json=body,
                           verify=False,
                           headers={'Authorization': 'Bearer {}'.format(token)})
    print(result.text)
    print(result.status_code)


if __name__ == '__main__':
    connect_to_hivemq()
    client.loop_start()
    while not connected_to_hive:
        continue
    client.on_message = on_message
    client.subscribe("test", qos=1)
    app.run(debug=False)
    client.loop_stop()
