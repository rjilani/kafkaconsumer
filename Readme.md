# A demo of Kafka consumer

## How to build application

`mvn clean package`

## How to run application

`java -jar target/consumer_kafka.jar`

### Or

`java -jar target/consumer_kafka.jar [groupid] [latest|earliest]`

## How to start zookeeper and kafka servers

`zookeeper-server-start.bat ../../config/zookeeper.properties`

`kafka-server-start.bat ../../config/server.properties`


## How to create topic

`kafka-topics.bat --create --topic quickstart-events --bootstrap-server localhost:9092`

`kafka-topics.bat --create --topic topic2 --bootstrap-server localhost:9092 --partitions 2`

## How to test the topic using producer and consumer command line utilities

`kafka-console-producer.bat --topic quickstart-events --bootstrap-server localhost:9092`

`kafka-console-consumer.bat --topic quickstart-events --from-beginning --bootstrap-server localhost:9092`

## How to stop zookeeper and kafka servers

`kafka-server-stop.bat`

`zookeeper-server-stop.bat`

## How to list all the topics

`kafka-topics.bat --list --bootstrap-server localhost:9092`

## How to list consumer groups and describes groups

`kafka-consumer-groups.bat  --list --bootstrap-server localhost:9092`

`kafka-consumer-groups.bat --describe --group mygroup --bootstrap-server localhost:9092`