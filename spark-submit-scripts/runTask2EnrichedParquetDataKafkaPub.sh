/usr/hdp/current/spark2-client/bin/spark-submit --files ./log4j.properties,applicationRemote.conf --driver-java-options "-Dlog4j.configuration=./log4j.properties" --conf "spark.executor.extraJavaOptions='-Dlog4j.configuration=./log4j.properties';spark.executor.logs.rolling.strategy=size;spark.executor.logs.rolling.maxSize=10485760;spark.executor.logs.rolling.maxRetainedFiles=100;spark.streaming.stopGracefullyOnShutdown=true"  --master yarn --deploy-mode client   --driver-memory 32g  --conf spark.driver.maxResultSize=6g   --executor-memory 8g --num-executors 12  --executor-cores 4 --class cs598ccc.task2.EnrichedParquetDataKafkaPublisher  --driver-class-path ~ --verbose  --name task2_setup cs598ccctask2.jar applicationRemote.conf