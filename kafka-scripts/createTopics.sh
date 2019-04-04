cd ~/opensource/apache/kafka_2.11-1.0.0/bin
./kafka-topics.sh --zookeeper localhost:2181 --create --if-not-exists --replication-factor 1 --partitions 1 --topic enriched-cleansed-data

./kafka-console-producer.sh --broker-list localhost:9092 --topic  enriched-cleansed-data


./kafka-topics.sh --zookeeper localhost:2181 --describe --topic enriched-cleansed-data

./kafka-topics.sh --zookeeper localhost:2181 --create --if-not-exists --replication-factor 1 --partitions 4 --topic enriched-cleansed-data-multipart

./kafka-topics.sh --zookeeper localhost:2181 --create --if-not-exists --replication-factor 1 --partitions 4 --topic query1dot1-multipart

./kafka-topics.sh --zookeeper localhost:2181 --create --if-not-exists --replication-factor 1 --partitions 4 --topic query1dot2-multipart

./kafka-topics.sh --zookeeper localhost:2181 --create --if-not-exists --replication-factor 1 --partitions 4 --topic query2dot1-multipart

./kafka-topics.sh --zookeeper localhost:2181 --create --if-not-exists --replication-factor 1 --partitions 4 --topic query2dot2-multipart

./kafka-topics.sh --zookeeper localhost:2181 --create --if-not-exists --replication-factor 1 --partitions 4 --topic query2dot4-multipart

