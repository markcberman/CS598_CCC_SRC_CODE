cd ~/opensource/apache/kafka_2.11-1.0.0/bin
cd /usr/hdp/current/kafka-broker/bin
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic enriched-cleansed-data-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic query1dot1-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic query1dot2-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic query2dot1-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic query2dot2-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic query2dot4-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic enriched-cleansed-data-2008-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic query3dot2-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic query3dot2-ns-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic enriched-cleansed-data-group-2-q1-q2-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic enriched-cleansed-data-group-2-q4-multipart





cd /usr/hdp/current/kafka-broker/bin
./kafka-console-consumer.sh --bootstrap-server node1.localdomain:6667 --from-beginning  --topic enriched-cleansed-data-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:6667 --from-beginning  --topic query1dot1-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:6667 --from-beginning  --topic query1dot2-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:6667 --from-beginning  --topic query2dot1-multipart

./kafka-console-consumer.sh --bootstrap-server localhost:6667 --from-beginning  --topic query2dot2-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:6667 --from-beginning  --topic query2dot4-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:6667 --from-beginning  --topic enriched-cleansed-data-2008-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:6667 --from-beginning  --topic query3dot2-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:6667 --from-beginning  --topic query3dot2-ns-multipart

./kafka-console-consumer.sh --bootstrap-server localhost:6667 --from-beginning  --topic enriched-cleansed-data-group-2-q1-q2-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:6667 --from-beginning  --topic enriched-cleansed-data-group-2-q4-multipart 



./kafka-console-consumer.sh --bootstrap-server localhost:6667 --from-beginning  --topic test

