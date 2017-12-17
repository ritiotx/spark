########################################################################
# This is part of implementation for Cloud + Edge = iotx Stage 2. Cloud is
# represented by Apache Spark and Edge computing framework is Calvin. This
# code focuses on subscribing to MQTT topic of Calvin, collect data and keep
# storing unique topic names corresponding to the receieved data
#
# iotx stage 2 demo
#
# Author: Aarti Gorade
# Email: ahg1512@rit.edu
#
#
########################################################################


from collections import deque

import paho.mqtt.client as mqtt


class PahoMQTTClass(mqtt.Client):
    """
    Paho mqtt client to connect to MQTT server to received data being
    published by Calvin and collects all different topic names
    """

    brokerFromCalvin = "iot.eclipse.org"
    portFromCalvin = 1883

    # set to store all unique topic names
    topicNames = set()

    # set to store all unique topic names
    mqttDataQueue = deque([])

    def on_message(self, mqttc, obj, msg):
        """
        Add topic name to the set when the data is received from MQTT server
        :param mqttc: mqtt client
        :param obj: received object
        :param msg: data mqtt payload
        :return: None
        """
        print("\n\n****INSIDE on_message")

        PahoMQTTClass.topicNames.add(msg.topic)
        print("PahoMQTTClass.topicNames = ",PahoMQTTClass.topicNames)
        PahoMQTTClass.mqttDataQueue.append(msg.payload)

    def run(self, broker, port, topic):
        """
        Connect to the MQTT broker and subscribe to the the topic to receive
        the data being published by Calvin continuously
        :return:
        """
        self.connect(broker, port, 60)
        self.subscribe(topic, 0)

        rc = 0
        while rc == 0:
            rc = self.loop()
        return rc
