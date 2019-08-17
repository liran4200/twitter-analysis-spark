# Twitter Analysis

## Running Commands:

1. start zookeeper server:
   `zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties`

2. start kafka server:
   `kafka-server-start /usr/local/etc/kafka/server.properties`

3. spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.2.0.jar twitter_consumer.py