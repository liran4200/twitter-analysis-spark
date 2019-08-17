# Twitter Analysis


## Running Commands:

1. start zookeeper server:
   `zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties`

2. start kafka server:
   `kafka-server-start /usr/local/etc/kafka/server.properties`

3. create topic:
   `kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test`

4. start twitter producer to kafka
   `python twitter_producer.py`

5. start twitter consumer to kafka
   `spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.2.0.jar twitter_consumer.py`

6. activate producer 
   `kafka-console-producer --broker-list localhost:9092 --topic test`

7. acivate consumer
   `kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning`