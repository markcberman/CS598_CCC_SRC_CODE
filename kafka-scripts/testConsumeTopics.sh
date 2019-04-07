cd ~/opensource/apache/kafka_2.11-1.0.0/bin
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic enriched-cleansed-data-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic query1dot1-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic query1dot2-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic query2dot1-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic query2dot2-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic query2dot4-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic enriched-cleansed-data-2008-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic query3dot2-multipart
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning  --topic query3dot2-ns-multipart

