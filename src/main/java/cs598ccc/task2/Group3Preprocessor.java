package cs598ccc.task2;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;

public class Group3Preprocessor {


    private static Logger logger = Logger.getLogger(Group3Preprocessor.class);
    private Properties prop = new Properties();
    private String master = null;
    private String startingOffsets = null;
    private String kafkaHost = null;
    private String enrichedDataKafkaTopic = null;
    private Integer enrichedDataKafkaTopicMinmaxOffsetsPerTrigger = null;
    private Integer enrichedDataKafkaTopicMinPartitions = null;
    private String enrichedData2008KafkaTopic = null;
    private String enrichedData2008CheckpointLocation = null;
    private Integer enrichedData2008TriggerProcessingTimeMillis =null;


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

            Group3Preprocessor instance = new Group3Preprocessor();
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
        enrichedData2008KafkaTopic = prop.getProperty("enrichedData2008KafkaTopic", "enriched-cleansed-data-2008-multipart");
        logger.info("enrichedData2008KafkaTopic: " + enrichedData2008KafkaTopic);
        enrichedData2008CheckpointLocation = prop.getProperty("enrichedData2008CheckpointLocation", "~/checkpoint/enrichedData2008");
        logger.info("enrichedData2008CheckpointLocation: " + enrichedData2008CheckpointLocation);
        enrichedData2008TriggerProcessingTimeMillis = Integer.valueOf(prop.getProperty("enrichedData2008TriggerProcessingTimeMillis", "1000"));


        if (input != null) {
            input.close();
        }

    }



    private void start() throws StreamingQueryException {

        SparkSession spark = SparkSession.builder()
                .appName("Group3Preprocessor")
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


        Dataset<Row> kafka_input = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("subscribe", enrichedDataKafkaTopic.trim())
                .option("startingOffsets", startingOffsets)
                .option("minPartitions", enrichedDataKafkaTopicMinPartitions)
                .option("maxOffsetsPerTrigger", enrichedDataKafkaTopicMinmaxOffsetsPerTrigger)
                .load();

        Dataset<Row> enriched_ontime_perf_2008_df = kafka_input.selectExpr("CAST(key AS String)","CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
                .select(col("key"),from_json(col("value"), SchemaCreator.createSchemaWithId()).as("data"), col("timestamp"))
                .select("key","data.*", "timestamp")
                .where(col("Year").equalTo(2008));

/*
        enriched_ontime_perf_2008_df.writeStream()
                .outputMode("append")
                .format("console")
                .queryName("console")
                .start();
*/

        StreamingQuery enriched_ontime_perf_2008KafkaSink = enriched_ontime_perf_2008_df.selectExpr("CAST(key AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .outputMode("append")
                .queryName("enriched_ontime_perf_2008")
                .option("topic", enrichedData2008KafkaTopic.trim())
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("checkpointLocation", enrichedData2008CheckpointLocation)
                .trigger(Trigger.ProcessingTime(enrichedData2008TriggerProcessingTimeMillis.intValue(), TimeUnit.MILLISECONDS))
                .start();



        logger.info("Query id for streaming to Kafka sink is: " + enriched_ontime_perf_2008KafkaSink.id());

        logger.info("Query name for streaming to Kafka sink is: " + enriched_ontime_perf_2008KafkaSink.name());

        logger.info("Streaming to Kafka sink. Status is: " +  enriched_ontime_perf_2008KafkaSink.status());


        spark.streams().awaitAnyTermination();

    }







}
