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

public class EnrichedDataKafkaConsumer {

    private static Logger logger = Logger.getLogger(EnrichedDataKafkaConsumer.class);

    private Properties prop = new Properties();
    private String master = null;
    private String startingOffsets = null;
    private String kafkaHost = null;
    private String enrichedDataKafkaTopic = null;
    private Integer enrichedDataKafkaTopicMinmaxOffsetsPerTrigger = null;
    private Integer enrichedDataKafkaTopicMinPartitions = null;
    private String query1dot1CheckpointLocation = null;
    private String query1dot2CheckpointLocation = null;
    private String query1dot1KafkaTopic = null;
    private Integer query1dot1TriggerProcessingTimeMillis = null;
    private String query1dot2KafkaTopic = null;
    private Integer query1dot2TriggerProcessingTimeMillis = null;
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

            EnrichedDataKafkaConsumer instance = new EnrichedDataKafkaConsumer();
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
        query1dot1CheckpointLocation = prop.getProperty("query1dot1CheckpointLocation","/scratch/checkpoint/query1dot1");
        logger.info("query1dot1CheckpointLocation: " + query1dot1CheckpointLocation);
        query1dot2CheckpointLocation = prop.getProperty("query1dot2CheckpointLocation","/scratch/checkpoint/query1dot2");
        logger.info("query1dot2CheckpointLocation: " + query1dot2CheckpointLocation);
        query1dot1KafkaTopic = prop.getProperty("query1dot1KafkaTopic", "query1dot1-multipart");
        logger.info("query1dot1KafkaTopic: " + query1dot1KafkaTopic);
        query1dot1CheckpointLocation = prop.getProperty("query1dot1CheckpointLocation", "/scratch/checkpoint/query1dot1");
        logger.info("query1dot1CheckpointLocation: " + query1dot1CheckpointLocation);
        query1dot1TriggerProcessingTimeMillis = Integer.valueOf(prop.getProperty("query1dot1TriggerProcessingTimeMillis", "1000"));
        logger.info("departuresByAirportTriggerProcessingTimeMillis: " + query1dot1TriggerProcessingTimeMillis);
        query1dot2KafkaTopic = prop.getProperty("query1dot2KafkaTopic", "query1dot2-multipart");
        logger.info("query1dot2KafkaTopic: " + query1dot2KafkaTopic);
        query1dot2CheckpointLocation = prop.getProperty("query1dot2CheckpointLocation", "/scratch/checkpoint/query1dot2");
        logger.info("query1dot2CheckpointLocation: " + query1dot2CheckpointLocation);
        query1dot2TriggerProcessingTimeMillis = Integer.valueOf(prop.getProperty("query1dot2TriggerProcessingTimeMillis", "1000"));
        logger.info("query1dot2TriggerProcessingTimeMillis: " + query1dot2TriggerProcessingTimeMillis);
        sparkLogLevel = prop.getProperty("sparkLogLevel","WARN");
        logger.info("sparkLogLevel: " + sparkLogLevel);


        if (input != null) {
            input.close();
        }

    }

    private void start() throws StreamingQueryException{

        SparkSession spark = SparkSession.builder()
                .appName("EnrichedDataKafkaConsumer")
                .master(master)
                .getOrCreate();

        logger.info("SparkSession Started.");

        spark.sparkContext().setLogLevel(sparkLogLevel);

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
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("subscribe", enrichedDataKafkaTopic.trim())
                .option("startingOffsets", startingOffsets)
                .option("minPartitions", enrichedDataKafkaTopicMinPartitions)
                .option("maxOffsetsPerTrigger", enrichedDataKafkaTopicMinmaxOffsetsPerTrigger)
                .load();


        Dataset<Row> jsonified_data = kafka_input.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
                .select(from_json(col("value"),SchemaCreator.createSchemaWithId()).as("data"),col("timestamp"))
                .select("data.*", "timestamp");

        jsonified_data.printSchema();

        jsonified_data.dropDuplicates("id");

        logger.info("Kafka source is streaming: "+ jsonified_data.isStreaming());

        /*

        jsonified_data.writeStream()
                .outputMode("append")
                .format("console")
                .start();


        */

        Dataset<Row> airportArrivalsAndDepartures = jsonified_data.groupBy("origin")
                .agg(
                        sum(col("departure")).alias("departures"),
                        sum(col("arrival")).alias("arrivals")
                )
                .withColumn("totalArrivalsAndDepartures", expr("(arrivals+departures)"))
                .drop("departures")
                .drop("arrivals")
                .drop("arrival")
                .drop("departures")
                .orderBy(desc("totalArrivalsAndDepartures"))
                ;

        /*

        airportArrivalsAndDepartures.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        */

        Dataset<Row> airline_on_time_arrival_performance = jsonified_data
                .withColumn("group", lit(1))
                .withColumnRenamed("departureDelay", "avgOriginDelay")
                .withColumnRenamed("arrivalDelay", "avgDestDelay")
                .withColumn("totalTripDelay", expr("(DepDelay+ArrDelay)"))
                .withColumn("typicalTripTime", expr("((0.117*Distance) +(0.517 * (lon_origin - lon_dest)) + 42.3) + avgOriginDelay + avgDestDelay"))
                .withColumn("time_added", expr(("ActualElapsedTime + totalTripDelay - typicalTripTime")))
                //.orderBy(asc("Carrier"), asc("origin"), asc("dest"), desc("FlightDate"), asc("DepTime"))
                ;



        Dataset<Row> airlineOnTimeArrivalPerformance_df = airline_on_time_arrival_performance.groupBy("group","Carrier")
                .agg(
                        avg(col("time_added")).alias("avg_time_added")
                )
                .orderBy(asc("avg_time_added"))
                ;

        /*
        airlineOnTimeArrivalPerformance_df.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        */

        StreamingQuery query1Dot1KafkaSink = airportArrivalsAndDepartures.selectExpr("CAST(origin AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .outputMode("complete")
                .queryName("query1dot1")
                .option("topic", query1dot1KafkaTopic.trim())
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("checkpointLocation", query1dot1CheckpointLocation)
                //.trigger(Trigger.ProcessingTime(query1dot1TriggerProcessingTimeMillis.intValue(), TimeUnit.MILLISECONDS))
                .start();

        logger.info("Query id for streaming to Kafka sink is: " + query1Dot1KafkaSink.id());

        logger.info("Query name for streaming to Kafka sink is: " + query1Dot1KafkaSink.name());

        logger.info("Streaming to Kafka sink. Status is: " +  query1Dot1KafkaSink.status());

        logger.info(query1Dot1KafkaSink.lastProgress());


        StreamingQuery query1Dot2KafkaSink = airlineOnTimeArrivalPerformance_df.selectExpr("CAST(Carrier AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .outputMode("complete")
                .queryName("query1dot2")
                .option("topic", query1dot2KafkaTopic.trim())
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("checkpointLocation", query1dot2CheckpointLocation)
                //.trigger(Trigger.ProcessingTime(query1dot2TriggerProcessingTimeMillis.intValue(), TimeUnit.MILLISECONDS))
                .start();

        logger.info("Query id for streaming to Kafka sink is: " + query1Dot2KafkaSink.id());

        logger.info("Query name for streaming to Kafka sink is: " + query1Dot2KafkaSink.name());

        logger.info("Streaming to Kafka sink. Status is: " +  query1Dot2KafkaSink.status());

        logger.info(query1Dot2KafkaSink.lastProgress());

        spark.streams().active();

        spark.streams().awaitAnyTermination();


    }


}
