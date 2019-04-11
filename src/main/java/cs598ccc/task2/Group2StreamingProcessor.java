package cs598ccc.task2;
import org.apache.commons.lang3.exception.ExceptionUtils;
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
    private String query2dot1CheckpointLocation = null;
    private Integer query2dot1TriggerProcessingTimeMillis = null;
    private String query2dot2KafkaTopic = null;
    private String query2dot2CheckpointLocation = null;
    private Integer query2dot2TriggerProcessingTimeMillis = null;
    private String query2dot4KafkaTopic = null;
    private String query2dot4CheckpointLocation = null;
    private Integer  query2dot4TriggerProcessingTimeMillis = null;
    private String sparkLogLevel = null;





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
        originAirportsPath = prop.getProperty("originAirportsPath", "/home/markcb2/csv_data/task2/origin_airports");
        logger.info("originAirportsPath: " + originAirportsPath);
        destAirportsPath = prop.getProperty("destAirportsPath","/home/markcb2/csv_data/task2/dest_airports");
        logger.info("destAirportsPath: " + destAirportsPath);
        query2dot1KafkaTopic = prop.getProperty("query2dot1KafkaTopic", "query2dot1-multipart");
        logger.info("query2dot1KafkaTopic: " + query2dot1KafkaTopic);
        query2dot1CheckpointLocation = prop.getProperty("query2dot1CheckpointLocation","/scratch/checkpoint/query2dot1");
        logger.info("query2dot1CheckpointLocation: " + query2dot1CheckpointLocation);
        query2dot1TriggerProcessingTimeMillis = Integer.valueOf(prop.getProperty("query2dot1TriggerProcessingTimeMillis", "1000"));
        logger.info("query2dot1TriggerProcessingTimeMillis: " + query2dot1TriggerProcessingTimeMillis);
        query2dot2KafkaTopic = prop.getProperty("query2dot2KafkaTopic", "query2dot2-multipart");
        logger.info("query2dot2KafkaTopic: " + query2dot2KafkaTopic);
        query2dot2CheckpointLocation = prop.getProperty("query2dot2CheckpointLocation","/scratch/checkpoint/query2dot2");
        logger.info("query2dot2CheckpointLocation: " + query2dot2CheckpointLocation);
        query2dot2TriggerProcessingTimeMillis = Integer.valueOf(prop.getProperty("query2dot2TriggerProcessingTimeMillis", "1000"));
        logger.info("query2dot2TriggerProcessingTimeMillis: " + query2dot2TriggerProcessingTimeMillis);
        query2dot4KafkaTopic = prop.getProperty("query2dot4KafkaTopic", "query2dot4-multipart");
        logger.info("query2dot4KafkaTopic: " + query2dot4KafkaTopic);
        query2dot4CheckpointLocation = prop.getProperty("query2dot4CheckpointLocation","/scratch/checkpoint/query2dot4");
        logger.info("query2dot4CheckpointLocation: " + query2dot4CheckpointLocation);
        query2dot4TriggerProcessingTimeMillis = Integer.valueOf(prop.getProperty("query2dot4TriggerProcessingTimeMillis", "1000"));
        logger.info("query2dot4TriggerProcessingTimeMillis: " + query2dot4TriggerProcessingTimeMillis);
        sparkLogLevel = prop.getProperty("sparkLogLevel","WARN");
        logger.info("sparkLogLevel: " + sparkLogLevel);







        if (input != null) {
            input.close();
        }

    }
    private void start() throws StreamingQueryException {

        SparkSession spark = SparkSession.builder()
                .appName("Group2StreamingProcessor")
                .master(master)
                .getOrCreate();

        spark.sparkContext().setLogLevel(sparkLogLevel);

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


        Dataset<Row> query2_1_unfiltered_results_df = enriched_ontime_perf_df.join(orign_airports_df,enriched_ontime_perf_df.col("Origin").equalTo(orign_airports_df.col("origin_airport_code")))
                .groupBy(enriched_ontime_perf_df.col("Origin"),enriched_ontime_perf_df.col("Carrier"))
                .agg(
                        avg(enriched_ontime_perf_df.col("DepDelay")).alias("avgDepartureDelay")

                )
                .orderBy(asc("Origin"),asc("avgDepartureDelay"));


        Dataset<Row> query2_2_unfiltered_results_df = enriched_ontime_perf_df.join(dest_airports_df,enriched_ontime_perf_df.col("Origin").equalTo(dest_airports_df.col("dest_airport_code")))
                .groupBy(enriched_ontime_perf_df.col("Origin"),enriched_ontime_perf_df.col("Dest"))
                .agg(
                        avg(enriched_ontime_perf_df.col("DepDelay")).alias("avgDepartureDelay")

                )
                .orderBy(asc("Origin"),asc("avgDepartureDelay"));



        Dataset<Row> query2_4_unfiltered_results_df = enriched_ontime_perf_df
                .groupBy(enriched_ontime_perf_df.col("Origin"),enriched_ontime_perf_df.col("Dest"))
                .agg(
                        avg(enriched_ontime_perf_df.col("ArrDelay")).alias("avgArrivalDelay")

                )
                .orderBy(asc("Origin"),asc("Dest"));



        StreamingQuery query2Dot1KafkaSink = query2_1_unfiltered_results_df.selectExpr("CAST(Origin AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .outputMode("complete")
                .queryName("query2dot1")
                .option("topic", query2dot1KafkaTopic.trim())
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("checkpointLocation", query2dot1CheckpointLocation)
                //.trigger(Trigger.ProcessingTime(query2dot1TriggerProcessingTimeMillis.intValue(), TimeUnit.MILLISECONDS))
                .start();

        logger.info("Query id for streaming to Kafka sink is: " + query2Dot1KafkaSink.id());

        logger.info("Query name for streaming to Kafka sink is: " + query2Dot1KafkaSink.name());

        logger.info("Streaming to Kafka sink. Status is: " +  query2Dot1KafkaSink.status());



        StreamingQuery query2Dot2KafkaSink = query2_2_unfiltered_results_df.selectExpr("CAST(concat(Origin,Dest) AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .outputMode("complete")
                .queryName("query2dot2")
                .option("topic", query2dot2KafkaTopic.trim())
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("checkpointLocation", query2dot2CheckpointLocation)
                //.trigger(Trigger.ProcessingTime(query2dot2TriggerProcessingTimeMillis.intValue(), TimeUnit.MILLISECONDS))
                .start();



        logger.info("Query id for streaming to Kafka sink is: " + query2Dot2KafkaSink.id());

        logger.info("Query name for streaming to Kafka sink is: " + query2Dot2KafkaSink.name());

        logger.info("Streaming to Kafka sink. Status is: " +  query2Dot2KafkaSink.status());




        StreamingQuery query2Dot4KafkaSink = query2_4_unfiltered_results_df.selectExpr("CAST(concat(Origin,Dest) AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .outputMode("complete")
                .queryName("query2dot4")
                .option("topic", query2dot4KafkaTopic.trim())
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("checkpointLocation", query2dot4CheckpointLocation)
                //.trigger(Trigger.ProcessingTime(query2dot4TriggerProcessingTimeMillis.intValue(), TimeUnit.MILLISECONDS))
                .start();



        logger.info("Query id for streaming to Kafka sink is: " + query2Dot4KafkaSink.id());

        logger.info("Query name for streaming to Kafka sink is: " + query2Dot4KafkaSink.name());

        logger.info("Streaming to Kafka sink. Status is: " +  query2Dot4KafkaSink.status());





        spark.streams().awaitAnyTermination();

    }

}
