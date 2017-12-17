########################################################################
# This is implementation for Cloud + Edge = iotx Stage 2. Cloud is represented
# by Apache Spark and Edge computing framework is Calvin. Apache Spark is
# receiving temperature data from Calvin via MQTT (pub/sub model). This
# program tracks sequence of operations in JSON format at Apache Spark and
# send it to Calvin via MQTT. Only Paho MQTT client package is used to
# generate DStream and collect topic names.
#
# iotx stage 2 demo
#
# Author: Aarti Gorade
# Email: ahg1512@rit.edu
#
# Invocation:
# Note: need to change source file in Dockerfile
# Docker image: aarti/sparkstage2-iotx
# Docker file: DockerfileSparkMQTTStage2
#
# OR
#
# Command line:
#   ./sbin/start-master.sh
#   ./bin/spark-class org.apache.spark.deploy.worker.Worker spark://<Spark
# Master's Ip address>:<Spark Master's Port>
#   ./bin/spark-submit
# --packages org.apache.spark:spark-streaming-mqtt-assembly_2.11:1.5.0
# python/SparkMQTTStage2_1.py
#
########################################################################

import socket
from collections import deque
from threading import Thread
from time import sleep

import paho.mqtt.client as mqtt

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.mqtt import MQTTUtils


def addToQueue(rdd):
    """
    This is the function responsible for adding calculated average values into
    the queue
    :param rdd: RDD containing calculated average values
    :return: None
    """

    rddList = rdd.collect()
    subList = [float(x[0]) for x in rddList]
    CalvinToSpark.queue.extend(subList)


class CalvinToSpark:

    # MQTT client
    mqttc = None

    # Queue to store calculated average values
    queue = deque([])

    # Spark Broker details
    sparkBroker = "iot.eclipse.org"
    sparkPort = 1883
    sparkTopic = "edu/rit/iotx/cloud/average/temperature"

    # Calvin broker URI
    brokerUrl = "tcp://iot.eclipse.org:1883"
    # Topic pattern where preprocessed temperature data is being published
    # from Calvin
    topic = "edu/rit/iotx/mapped"

    # counters to keep track of running sum and count to calculate average value
    sumAccum = 0
    countAccum = 0

    # window and sliding interval using for calculating average over each window of
    # incoming Spar Stream
    windowInterval = 30
    slidingInterval = 15

    # flag for stop modifying DAG
    stopModificationDag = False

    # counters to keep track of running sum and count to calculate average value
    sumAccum = 0
    countAccum = 0

    def getHostIpAddress(self):
        """
        Get global Ip Address of the current machine
        :return: Ip address
        """

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip


    def connectToBroker(self, broker, port):
        """
        This is the function responsible for creating MQTT client and connecting to
        the give broker server on desired port
        :param broker: broker server
        :param port: port to connect to
        :return: None
        """

        CalvinToSpark.mqttc = mqtt.Client()
        print "Trying to connect to broker..."
        CalvinToSpark.mqttc.connect(broker, port)
        print "Successfully connected!!!"


    def publishResults(self):
        """
        This is the function responsible for fetching data from queue and publishing it using MQTT
        :return: None
        """

        print("\n\nPublishing results...")
        # global mqttc
        # global queue
        mqttClient = CalvinToSpark.mqttc

        while True:
            while not (CalvinToSpark.queue):
                sleep(CalvinToSpark.slidingInterval)

            data = CalvinToSpark.queue.popleft()
            print(data)
            mqttClient.publish(CalvinToSpark.sparkTopic, data)


    def update(self, x):
        """
        Add the incoming new item in current sliding window interval into the sum
        :param x: new value
        :return: current average value
        """

        # global sumAccum
        # global countAccum
        CalvinToSpark.sumAccum += x
        CalvinToSpark.countAccum += 1
        return (CalvinToSpark.sumAccum / CalvinToSpark.countAccum)


    def reverseUpdate(self, x):
        """
        Remove item from old sliding window interval from current sum
        :param x: old item from last window interval
        :return: current average value
        """

        # global sumAccum
        # global countAccum
        CalvinToSpark.sumAccum -= x
        CalvinToSpark.countAccum -= 1
        return (CalvinToSpark.sumAccum / CalvinToSpark.countAccum)

    def evaluate(self):

        print("\n\n********* Inside evaluate.....")

        # Load spark streaming mqtt package at runtime


        # Ip address and port number for Spark cluster
        hostAddress = self.getHostIpAddress()
        hostPort = "7077"

        print("creating SparkContext...")
        # connect to Spark cluster "spark:cluster-host:port"
        sc = SparkContext("spark://" + hostAddress + ":" +
                          hostPort,
                          appName="SparkMQTTStage2_1")
        sc.setLogLevel("ERROR")

        print("Created Streaming context...")
        # reading data every 15 seconds
        ssc = StreamingContext(sc, 15)

        # mandatory to store checkpointed data for Spark Streaming
        # ssc.checkpoint("/Users/Aarti/IdeaProjects/SparkCheckpointedData")
        ssc.checkpoint("../tmp/SparkCheckpointedData")

        print("Creating MQTT stream...")
        mqttStream = MQTTUtils.createStream(ssc, CalvinToSpark.brokerUrl,
                                               CalvinToSpark.topic)


        # Convert incoming stream items to float values
        farenheitTemp = mqttStream.map(lambda line: (float(line),1))

        # lambda functions to calculate average using windowing technique
        update_1 = lambda x, y: self.update(x)
        reverseUpdate_1 = lambda x, y: self.reverseUpdate(x)

        # Reduce last 30 seconds of data, every 15 seconds
        windowedWordCounts = farenheitTemp.reduceByKeyAndWindow(update_1,
                                                                reverseUpdate_1,
                                                                CalvinToSpark.windowInterval,
                                                                CalvinToSpark.slidingInterval)

        # connect to broker
        self.connectToBroker(CalvinToSpark.sparkBroker,
                             CalvinToSpark.sparkPort)

        # foreachRDD is Action. Add each RDD containing average values into the
        # queue
        windowedWordCounts.foreachRDD(addToQueue)

        # create worker thread to fetch data from queue and publish it to broker
        # using MQTT
        worker = Thread(target=self.publishResults)
        worker.setDaemon(True)
        worker.start()

        # Start spark streaming jobs
        print("\nSpark jobs starting ...")
        ssc.start()
        print("\nSpark jobs waiting for termination...")

        # wait for 100 seconds before terminating Spark job execution
        ssc.awaitTermination()