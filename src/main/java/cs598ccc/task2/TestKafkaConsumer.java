package cs598ccc.task2;


import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.apache.commons.lang3.exception.ExceptionUtils;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class TestKafkaConsumer {

    private static Logger logger = Logger.getLogger(TestKafkaConsumer.class);

    public static void main(String[] args) throws StreamingQueryException{

        SparkSession spark = SparkSession.builder()
                .appName("EnrichedDataKafkaConsumer")
                .master("local[*]")
                .getOrCreate();

        logger.info("SparkSession Started.");

        spark.sparkContext().setLogLevel("TRACE");

        spark.streams().addListener(new StreamingQueryListener() {
            @Override
            public void onQueryStarted(QueryStartedEvent queryStarted) {
                logger.info("Streaming Query started: " + queryStarted.id());
            }
            @Override
            public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
                logger.info("Streaming Query terminated: " + queryTerminated.id());
            }
            @Override
            public void onQueryProgress(QueryProgressEvent queryProgress) {
                logger.info("Streaming Query made progress: " + queryProgress.progress());
            }
        });

        Dataset<Row> kafka_input = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "enriched-cleansed-data-multipart")
                .option("startingOffsets", "earliest")
                .option("minPartitions", 4)
                .option("maxOffsetsPerTrigger", 100000)
                .load();


        Dataset<Row> jsonified_data = kafka_input.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
                .select(from_json(col("value"),SchemaCreator.createSchemaWithId()).as("data"),col("timestamp"))
                .select("data.*", "timestamp");

        jsonified_data.printSchema();

        jsonified_data.dropDuplicates("id");

        logger.info("Kafka source is streaming: "+ jsonified_data.isStreaming());



        jsonified_data.writeStream()
                .outputMode("append")
                .format("console")
                .start();


        spark.streams().active();

        spark.streams().awaitAnyTermination();


    }

}
