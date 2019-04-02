package cs598ccc.task2;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.spark.sql.streaming.Trigger;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class Group2StreamingProcessor {

    private static Logger logger = Logger.getLogger(Group2StreamingProcessor.class);

    private Properties prop = new Properties();
    private String master = null;
    private String startingOffsets = null;
    private String kafkaHost = null;
    private String enrichedDataKafkaTopic = null;
    private Integer enrichedDataKafkaTopicMinmaxOffsetsPerTrigger = null;
    private Integer enrichedDataKafkaTopicMinPartitions = null;
    private String originAirportsPath = null;
    private String destAirportsPath = null;
    private String query2dot1KafkaTopic = null;
    private Integer query2dot1KafkaTopicMinPartitions = null;
    private Integer query2dot1KafkaTopicMinmaxOffsetsPerTrigger = null;
    private String query2dot1CheckpointLocation = null;
    private Integer query2dot1TriggerProcessingTimeMillis = null;





    public static void main(String[] args){

        String propertyFileName = null;
        if (args.length != 1) {
            propertyFileName = "~/application.conf";
            logger.info("Fully Qualified Path and File Name for the application properties file is missing from the command line for the configuration file. Using the default application properties file:  " + propertyFileName);
        } else {
            propertyFileName = args[0];
            logger.info("Using the application properties file specified on the command line: " + propertyFileName);
        }

        try {

            Group2StreamingProcessor instance = new Group2StreamingProcessor();
            instance.loadProperties(propertyFileName);
            instance.start();
        }
        catch(IOException ioe){
            logger.error(ExceptionUtils.getStackTrace(ioe));
        }
        catch(NumberFormatException nfe){
            logger.error(ExceptionUtils.getStackTrace(nfe));
        }
        catch(StreamingQueryException sqe){
            logger.error(ExceptionUtils.getStackTrace(sqe));
        }
        catch(Exception e){
            logger.error(ExceptionUtils.getStackTrace(e));
        }


    }


    private void loadProperties(String propertyFileName) throws IOException, NumberFormatException {

        InputStream input = new FileInputStream(propertyFileName);

        // load a properties file
        prop.load(input);
        master = prop.getProperty("master", "local[*]");
        logger.info("master: " + master);
        startingOffsets = prop.getProperty("startingOffsets", "latest");
        logger.info("startingOffsets: " + startingOffsets);
        kafkaHost = prop.getProperty("kafkaHost", "localhost:6667");
        logger.info("kafkaHost: " + kafkaHost);
        enrichedDataKafkaTopic = prop.getProperty("enrichedDataKafkaTopic");
        logger.info("enrichedDataKafkaTopic: " + enrichedDataKafkaTopic);
        enrichedDataKafkaTopicMinPartitions =  Integer.valueOf(prop.getProperty("enrichedDataKafkaTopicMinPartitions", "1"));
        logger.info("enrichedDataKafkaTopicMinPartitions: " + enrichedDataKafkaTopicMinPartitions);
        enrichedDataKafkaTopicMinmaxOffsetsPerTrigger =  Integer.valueOf(prop.getProperty("enrichedDataKafkaTopicMinmaxOffsetsPerTrigger", "100000"));
        logger.info("enrichedDataKafkaTopicMinmaxOffsetsPerTrigger: " + enrichedDataKafkaTopicMinmaxOffsetsPerTrigger);
        originAirportsPath = prop.getProperty("originAirportsPath", "hdfs:///cs598ccc/ref_data/origin_airports");
        logger.info("originAirportsPath: " + originAirportsPath);
        destAirportsPath = prop.getProperty("destAirportsPath","hdfs:///cs598ccc/ref_data/dest_airports");
        logger.info("destAirportsPath: " + destAirportsPath);
        query2dot1KafkaTopic = prop.getProperty("query2dot1KafkaTopic", "query2dot1-multipart");
        logger.info("query2dot1KafkaTopic: " + query2dot1KafkaTopic);
        query2dot1CheckpointLocation = prop.getProperty("query2dot1CheckpointLocation","~/checkpoint/query2dot1");
        logger.info("query2dot1CheckpointLocation: " + query2dot1CheckpointLocation);
        query2dot1TriggerProcessingTimeMillis = Integer.valueOf(prop.getProperty("query2dot1TriggerProcessingTimeMillis", "1000"));
        logger.info("query2dot1TriggerProcessingTimeMillis: " + query2dot1TriggerProcessingTimeMillis);





        if (input != null) {
            input.close();
        }

    }
    private void start() throws StreamingQueryException {

        SparkSession spark = SparkSession.builder()
                .appName("EnrichedDataKafkaConsumer")
                .master(master)
                .getOrCreate();

        logger.info("SparkSession Started.");

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

        Dataset<Row> orign_airports_df = spark.read()
                .format("csv")
                .option("sep", ",")
                .option("header", "true")
                .load(originAirportsPath);

        //orign_airports_df.createOrReplaceTempView("origin_airports");

        Dataset<Row> dest_airports_df = spark.read()
                .format("csv")
                .option("sep", ",")
                .option("header", "true")
                .load(destAirportsPath);

        //dest_airports_df.createOrReplaceTempView("dest_airports");

        Dataset<Row> kafka_input = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("subscribe", enrichedDataKafkaTopic.trim())
                .option("startingOffsets", startingOffsets)
                .option("minPartitions", enrichedDataKafkaTopicMinPartitions)
                .option("maxOffsetsPerTrigger", enrichedDataKafkaTopicMinmaxOffsetsPerTrigger)
                .load();


        Dataset<Row> enriched_ontime_perf_df = kafka_input.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
                .select(from_json(col("value"), SchemaCreator.createSchemaWithId()).as("data"), col("timestamp"))
                .select("data.*", "timestamp");

        enriched_ontime_perf_df.printSchema();

        enriched_ontime_perf_df.dropDuplicates("id");

        /*
        enriched_ontime_perf_df.writeStream()
                .outputMode("append")
                .format("console")
                .start();
        */

        Dataset<Row> query2_1_unfiltered_results_df = enriched_ontime_perf_df.join(orign_airports_df,enriched_ontime_perf_df.col("Origin").equalTo(orign_airports_df.col("origin_airport_code")))
                .groupBy(enriched_ontime_perf_df.col("Origin"),enriched_ontime_perf_df.col("Carrier"))
                .agg(
                        avg(enriched_ontime_perf_df.col("DepDelay")).alias("avgDepartureDelay")

                )
                .orderBy(asc("Origin"),asc("avgDepartureDelay"));

        /*
        query2_1_unfiltered_results_df.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        */

        StreamingQuery query2Dot1KafkaSink = query2_1_unfiltered_results_df.selectExpr("CAST(Origin AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .outputMode("complete")
                .queryName("query2dot1")
                .option("topic", query2dot1KafkaTopic.trim())
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("checkpointLocation", query2dot1CheckpointLocation)
                .trigger(Trigger.ProcessingTime(query2dot1TriggerProcessingTimeMillis.intValue(), TimeUnit.MILLISECONDS))
                .start();

        logger.info("Query id for streaming to Kafka sink is: " + query2Dot1KafkaSink.id());

        logger.info("Query name for streaming to Kafka sink is: " + query2Dot1KafkaSink.name());

        logger.info("Streaming to Kafka sink. Status is: " +  query2Dot1KafkaSink.status());

        spark.streams().awaitAnyTermination();

    }

}
