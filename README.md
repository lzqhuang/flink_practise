##创建topic 
./bin/kafka-topics.sh --create --topic data --partitions 3 --replication-factor 2 --zookeeper bigdata01:2181,bigdata02:2181,bigdata03:2181

#消费topic
./bin/kafka-console-consumer.sh --bootstrap-server bigdata01:9092,bigdata02:9092,bigdata03:9092 --topic data