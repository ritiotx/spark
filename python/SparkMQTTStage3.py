########################################################################
# This is implementation for Stage 3 Edge-only approach. Cloud is represented
# by Apache Spark and Edge computing framework is Calvin. Apache Spark is
# receiving temperature data from Calvin via MQTT (pub/sub model). This
# program tracks sequence of operations in JSON format at Apache Spark and
# send it to Calvin via MQTT. Only Paho MQTT client package is used to
# generate DStream and collect topic names.
#
# iotx stage 3 demo
#
# Author: Aarti Gorade
# Email: ahg1512@rit.edu
#
# Invocation:
#
# Docker image: aarti/sparkstage3-iotx
# Docker file: DockerfileSparkMQTTStage3
#
# OR
#
# Command line:
#   ./sbin/start-master.sh
#   ./bin/spark-class org.apache.spark.deploy.worker.Worker spark://<Spark
# Master's Ip address>:<Spark Master's Port>
#   ./bin/spark-submit
# --packages org.apache.spark:spark-streaming-mqtt-assembly_2.11:1.5.0 python/SparkMQTTStage3.py
#
########################################################################

import hashlib
import json
import socket
import threading
from collections import deque
from threading import Thread
from time import sleep

import paho.mqtt.client as mqtt

import PahoMQTT
from pyspark import SparkContext
from pyspark.streaming import DStream
from pyspark.streaming import StreamingContext

# MQTT client
mqttc = None

# Queue to store calculated average values
queue = deque([])

# Calvin Broker details
broker = "iot.eclipse.org"
# port number
port = 1883
# Calvin broker URI
brokerUrl = "tcp://iot.eclipse.org:1883"
# Topic from where temperature data is being received
topic = "edu/rit/iotx/+/temperature"

# Spark Broker details
sparkBroker = "iot.eclipse.org"
# Spark broker port
sparkPort = 1883
# Spark mqtt topic where directed acyclic graph information is being sent
sparkTopic = "edu/rit/iotx/cloud/dag"


def getHostIpAddress():
    """
    Get global Ip Address of the current machine
    :return: Ip address
    """

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip


# Ip address and port number for Spark cluster
hostAddress = getHostIpAddress()
hostPort = "7077"


def connectToBroker(broker, port):
    """
    This is the function responsible for creating MQTT client and connecting to
    the give broker server on desired port
    :param broker: broker server
    :param port: port to connect to
    :return: None
    """

    global mqttc
    mqttc = mqtt.Client()
    print "Trying to connect to broker..."
    mqttc.connect(broker, port)
    print "Successfully connected!!!"

    childInfo = {}
    # if hasattr(child, "prev"):
    childInfo["seqNum"] = DStream.sequenceNum
    DStream.sequenceNum += 1
    childInfo["operation"] = "publish"
    sink = {}
    sink["type"] = "MQTT"
    sink["address"] = sparkBroker + ":" + str(sparkPort)
    sink["channel"] = sparkTopic
    childInfo["sink"] = sink
    childInfo["parent"] = DStream.parentId

    childInfo["uid"] = hashlib.sha224(
        childInfo["operation"] + sink["type"] + sink["address"] + sink[
            "channel"] + childInfo["parent"]).hexdigest()

    DStream.parentId = childInfo["uid"]
    DStream.sparkDAG.append(childInfo)


def updateTopicNames():
    """
    Different topics on which data is being received are stored and kept
    up-to-date
    :return: None
    """
    source = DStream.sparkDAG[0]
    source["source"]["channel"] = list(PahoMQTT.PahoMQTT.topicNames)

    source["uid"] = hashlib.sha224(
        source["operation"] + source["source"]["type"] + source["source"][
            "address"] + str(
            len(source["source"]["channel"]))).hexdigest()
    DStream.sparkDAG[0] = source
    child = DStream.sparkDAG[1]
    child["parent"] = source["uid"]
    DStream.sparkDAG[1] = child


def extractDag():
    """
    Extract Directed Acyclic graph and offload to Edge in JSON format
    :return: Serialized JSON Directed Acyclic Graph
    """

    updateTopicNames()
    return json.dumps(DStream.sparkDAG)


def addToQueue():
    """
    This is the function responsible for adding extracted DAG JSON into the
    queue
    :return: None
    """

    # Keep adding newly extracted DAG JSON in queue
    while True:
        global queue
        queue.append(extractDag())
        # wait for 5 seconds before queueing next DAG JSON
        sleep(5)


def publishFromQueue():
    """
    This is the function responsible for fetching data from queue and publishing
     it using MQTT
    :return: None
    """

    global queue
    # keep publishing DAG JSON
    while True:
        # wait for 15 seconds before publishing next DAG JSON
        while not (queue):
            sleep(15)

        data = queue.popleft()
        print(data)
        mqttc.publish(sparkTopic, data)


def getTopicNames():
    """
    Get topic names from received MQTT payload
    :return: None
    """
    mqttTopicClient = PahoMQTT.PahoMQTT()
    rc = mqttTopicClient.run(broker, port, topic)


def WriteDataToSocket():
    """
    Data received from MQTT broker is written to socket to generate DStream
    :return: None
    """

    port = 9999                    # Reserve a port for your service.
    s = socket.socket()             # Create a socket object
    host = socket.gethostname()     # Get local machine name
    s.bind(("localhost", port))            # Bind to the port
    s.listen(5)                     # Now wait for client connection.

    while True:
        conn, addr = s.accept()     # Establish connection with client.
        pahoMqttQueue = PahoMQTT.PahoMQTT().mqttDataQueue
        while True:
            while not(pahoMqttQueue):
                sleep(1)

            data = pahoMqttQueue.popleft()
            conn.send(data+"\n")

        conn.send('Thank you for connecting')
        conn.close()


def collectDataFromMqttBroker():
    """
    Collects data from MQTT broker using Paho Client
    :return: None
    """
    mqttTopicClient = PahoMQTT.PahoMQTT()
    rc = mqttTopicClient.run(mqttTopicClient.brokerFromCalvin,
                             mqttTopicClient.portFromCalvin, topic)


def getMqttData():
    """
    Collects data from MQTT broker using Paho Client and Write data to socket to
     generate DStream
    :return: None
    """

    collectDataFromMqttBrokerWorker = Thread(target=collectDataFromMqttBroker)
    collectDataFromMqttBrokerWorker.setDaemon(True)
    collectDataFromMqttBrokerWorker.start()
    sleep(2)
    writeDataToSocketWorker = Thread(target=WriteDataToSocket)
    writeDataToSocketWorker.setDaemon(True)
    writeDataToSocketWorker.start()


if __name__ == "__main__":
    '''
    This is the main function responsible for collecting DAG from Spark and off 
    loading to Calvin client to perform evaluation
    '''

    # connect to Spark cluster "spark:cluster-host:port"
    sc = SparkContext("spark://" + hostAddress + ":" + hostPort, appName="iotx")
    sc.setLogLevel("ERROR")

    print("Created Streaming context...")
    # reading data every 15 seconds
    ssc = StreamingContext(sc, 15)

    # mandatory to store checkpointed data for Spark Streaming
    ssc.checkpoint("/tmp/SparkCheckpointedData")

    # # create worker thread to fetch topic names from Calvin topic and store in
    # # dictionary
    # getCalvinTopics = Thread(target=getTopicNames)
    # getCalvinTopics.setDaemon(True)
    # getCalvinTopics.start()

    collectMqttDataWorker = Thread(target=getMqttData)
    collectMqttDataWorker.setDaemon(True)
    collectMqttDataWorker.start()

    host = socket.gethostname()     # Get local machine name
    port = 9999                    # Reserve a port for your service.

    print("Creating DStream ...")
    mqttStream = ssc.socketTextStream("localhost", port)

    # Convert incoming stream items to float values
    #celsiusTemp = mqttStream.map(lambda line: float(line))

    # Convert Celsius to Farenheit and store each value in pair format
    # farenheitTemp = celsiusTemp.map(
    #     lambda temp: (str(((temp[0]) * 9 / 5) + 32).decode("utf-8"), 1))

    farenheitTemp = mqttStream.map(
        lambda temp: ((temp * 9 / 5) + 32), 1)

    # If temperature is above some threshold, return false else true. Can be
    # used to turn on/off Edge device eg. heater at Smart Home
    threshold = 100.0
    result = farenheitTemp.map(lambda x: x>threshold)


    # perform print action
    result.pprint()

    # connect to broker
    connectToBroker(sparkBroker, sparkPort)

    # Worker thread to perform operation to add newly extracted data into queue
    t = threading.Timer(10.0, addToQueue)
    t.start()

    # Get DAG JSON from queue and publish to broker for Calvin usage
    publishFromQueue()
