package cs598ccc.task2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
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


public class EnrichedParquetDataKafkaPublisher {

    private static Logger logger = Logger.getLogger(EnrichedParquetDataKafkaPublisher.class);


    private Properties prop = new Properties();
    private String master = null;
    private Integer enrichedParquetDataMaxFilesPerTrigger = null;
    private String enrichedParquetDataPath = null;
    private String kafkaHost = null;
    private String enrichedDataKafkaTopic = null;
    private String enrichedParquetDataCheckpointLocation = null;
    private Integer enrichedParquetDataTriggerProcessingTimeMillis = null;
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

            EnrichedParquetDataKafkaPublisher instance = new EnrichedParquetDataKafkaPublisher();
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

    private void loadProperties(String propertyFileName) throws IOException, NumberFormatException{

        InputStream input = null;


            input = new FileInputStream(propertyFileName);

            // load a properties file
            prop.load(input);
            master = prop.getProperty("master", "local[*]");
            logger.info("master: " + master);
            enrichedParquetDataMaxFilesPerTrigger = Integer.valueOf((prop.getProperty("enrichedParquetDataMaxFilesPerTrigger", "4")));
            logger.info("enrichedParquetDataMaxFilesPerTrigger: " + enrichedParquetDataMaxFilesPerTrigger);
            enrichedParquetDataPath = prop.getProperty("enrichedParquetDataPath", "hdfs:///cs598ccc/parquet_data/enriched_ontimeperf");
            logger.info("enrichedParquetDataPath: " + enrichedParquetDataPath);
            kafkaHost = prop.getProperty("kafkaHost", "localhost:6667");
            logger.info("kafkaHost: " + kafkaHost);
            enrichedParquetDataCheckpointLocation = prop.getProperty("enrichedParquetDataCheckpointLocation", "/scratch/checkpoint/EnrichedParquetDataKafkaPublisher");
            logger.info("enrichedParquetDataCheckpointLocation: " + enrichedParquetDataCheckpointLocation);
            enrichedParquetDataTriggerProcessingTimeMillis =  Integer.valueOf(prop.getProperty("enrichedParquetDataTriggerProcessingTimeMillis", "1000"));
            logger.info("enrichedParquetDataTriggerProcessingTimeMillis: " + enrichedParquetDataTriggerProcessingTimeMillis);
            sparkLogLevel = prop.getProperty("sparkLogLevel","WARN");
            logger.info("sparkLogLevel: " + sparkLogLevel);



            if (input != null) {
                input.close();
            }

    }

    private void start() throws StreamingQueryException{

        SparkSession spark = SparkSession.builder()
                .appName("EnrichedParquetDataKafkaPublisher")
                .master(master)
                .getOrCreate();

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

        logger.info("SparkSession Started.");

        logger.info("Reading Enriched Parquet Files");

        Dataset<Row> enrichedData_df = spark.readStream()
                .schema(SchemaCreator.createSchema())
                .option("maxFilesPerTrigger", enrichedParquetDataMaxFilesPerTrigger.intValue())
                .parquet(enrichedParquetDataPath);


        enrichedData_df = enrichedData_df
                .withColumn("id", functions.hash(enrichedData_df.col("Year"),enrichedData_df.col("Month"), enrichedData_df.col("DayofMonth"),enrichedData_df.col("DepTime"),enrichedData_df.col("AirlineID"),enrichedData_df.col("FlightNum")));


        /*
        enrichedData_df.writeStream()
                .outputMode("append")
                .format("console")
                .queryName("console")
                .start();
        */


        StreamingQuery kafkaSink =  enrichedData_df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .queryName("enriched_data")
                .format("kafka")
                .option("topic", "enriched-cleansed-data-multipart")
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("checkpointLocation", enrichedParquetDataCheckpointLocation)
                .trigger(Trigger.ProcessingTime(enrichedParquetDataTriggerProcessingTimeMillis.intValue(), TimeUnit.MILLISECONDS))
                .start();

        logger.info("Query id for streaming to kafka sink is: " + kafkaSink.id());

        logger.info("Query name for streaming to kafka sink is: " + kafkaSink.name());

        logger.info("Streaming to kafka topic. Status is: " +  kafkaSink.status());


        spark.streams().awaitAnyTermination();




    }



}
