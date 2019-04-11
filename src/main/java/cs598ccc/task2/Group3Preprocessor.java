package cs598ccc.task2;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

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
    private String enrichedOntimePerf2Task2Subset = null;
    private Boolean shouldUse2008SubsetData = null;
    private String enrichedParquetDataPath = null;
    private String enrichedOntimePerf2Task2SubsetParquet = null;
    private Integer filterByMonth =null;
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
        enrichedData2008CheckpointLocation = prop.getProperty("enrichedData2008CheckpointLocation", "/scratch/checkpoint/enrichedData2008");
        logger.info("enrichedData2008CheckpointLocation: " + enrichedData2008CheckpointLocation);
        enrichedData2008TriggerProcessingTimeMillis = Integer.valueOf(prop.getProperty("enrichedData2008TriggerProcessingTimeMillis", "1000"));
        enrichedOntimePerf2Task2Subset = prop.getProperty("enrichedOntimePerf2Task2Subset", "hdfs:///cs598ccc/csv_data/task2/enriched_ontimeperf_task2_subset");
        logger.info("enrichedOntimePerf2Task2Subset: " + enrichedOntimePerf2Task2Subset);
        shouldUse2008SubsetData = Boolean.valueOf(prop.getProperty("shouldUse2008SubsetData","true"));
        logger.info("shouldUse2008SubsetData: " + shouldUse2008SubsetData);
        enrichedParquetDataPath = prop.getProperty("enrichedParquetDataPath", "hdfs:///cs598ccc/parquet_data/enriched_ontimeperf");
        logger.info("enrichedParquetDataPath: " + enrichedParquetDataPath);
        enrichedOntimePerf2Task2SubsetParquet = prop.getProperty("enrichedOntimePerf2Task2SubsetParquet",  "hdfs:///cs598ccc/parquet_data/enriched_ontimeperf_task2_subset");
        logger.info("enrichedOntimePerf2Task2SubsetParquet: " + enrichedOntimePerf2Task2SubsetParquet);
        filterByMonth = Integer.valueOf(prop.getProperty("filterByMonth","5"));
        logger.info("filterByMonth: " + filterByMonth);
        sparkLogLevel = prop.getProperty("sparkLogLevel","WARN");
        logger.info("sparkLogLevel: " + sparkLogLevel);


        if (input != null) {
            input.close();
        }

    }



    private void start() throws StreamingQueryException {

        SparkSession spark = SparkSession.builder()
                .appName("Group3Preprocessor")
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

        Dataset<Row> enriched_ontime_perf_2008_df = null;

        if (shouldUse2008SubsetData){
            logger.debug("Using 2008 subset data");

            /*
            enriched_ontime_perf_2008_df = spark.read()
                    .format("csv")
                    .option("sep", ",")
                    .option("header", "true")
                    .load(enrichedOntimePerf2Task2Subset)
            ;
            */

            enriched_ontime_perf_2008_df = spark.read().format("parquet")
                    .load(enrichedOntimePerf2Task2SubsetParquet);
            enriched_ontime_perf_2008_df.printSchema();

        }
        else{
            logger.debug("Using 2008 full data");
            enriched_ontime_perf_2008_df = spark.read().format("parquet")
                    .load(enrichedParquetDataPath)
                    .withColumn("id", functions.hash(col("Year")
                            ,col("Month")
                            ,col("DayofMonth")
                            ,col("DepTime")
                            ,col("AirlineID")
                            ,col("FlightNum")
                            )
                    )
                    .where(col("Year").equalTo(2008))
                    ;
        }

        logger.info(enriched_ontime_perf_2008_df.count() + " records read.");

        enriched_ontime_perf_2008_df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
                .write()
                .format("kafka")
                .option("topic", enrichedData2008KafkaTopic.trim())
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("checkpointLocation", enrichedData2008CheckpointLocation)
                .save();

        logger.info("Wrote " + enriched_ontime_perf_2008_df.count() + " 2008 enriched on-time performance records to kafka topic:  " + enrichedData2008KafkaTopic);

    }







}
