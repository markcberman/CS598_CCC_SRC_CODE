package cs598ccc.task2;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
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

public class Group3StreamingProcessorHybridJoin {
    private static Logger logger = Logger.getLogger(Group3StreamingProcessorHybridJoin.class);

    private Properties prop = new Properties();
    private String master = null;
    private String startingOffsets = null;
    private String kafkaHost = null;
    private String enrichedData2008KafkaTopic = null;
    private Integer enrichedData2008KafkaTopicMinmaxOffsetsPerTrigger = null;
    private Integer enrichedData2008KafkaTopicMinPartitions = null;
    private Integer enrichedData2008TriggerProcessingTimeMillis = null;
    private String query3dot2KafkaTopic = null;
    private String query3dot2CheckpointLocation = null;
    private Integer query3dot2TriggerProcessingTimeMillis =null;
    private String enrichedParquetDataPath = null;
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

            Group3StreamingProcessorHybridJoin instance = new Group3StreamingProcessorHybridJoin();
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
        enrichedData2008KafkaTopic = prop.getProperty("enrichedData2008KafkaTopic", "enriched-cleansed-data-2008-multipart");
        logger.info("enrichedData2008KafkaTopic: " + enrichedData2008KafkaTopic);
        enrichedData2008KafkaTopicMinPartitions =  Integer.valueOf(prop.getProperty("enrichedData2008KafkaTopicMinPartitions", "1"));
        logger.info("enrichedData2008KafkaTopicMinPartitions: " + enrichedData2008KafkaTopicMinPartitions);
        enrichedData2008KafkaTopicMinmaxOffsetsPerTrigger =  Integer.valueOf(prop.getProperty("enrichedData2008KafkaTopicMinmaxOffsetsPerTrigger", "10000"));
        logger.info("enrichedData2008KafkaTopicMinmaxOffsetsPerTrigger: " + enrichedData2008KafkaTopicMinmaxOffsetsPerTrigger);
        query3dot2KafkaTopic = prop.getProperty("query3dot2KafkaTopic", "query3dot2-multipart");
        logger.info("query3dot2KafkaTopic: " + query3dot2KafkaTopic);
        query3dot2CheckpointLocation = prop.getProperty("query3dot2CheckpointLocation", "/scratch/checkpoint/query3dot2");
        logger.info("query3dot2CheckpointLocation: " + query3dot2CheckpointLocation);
        query3dot2TriggerProcessingTimeMillis = Integer.valueOf(prop.getProperty("query3dot2TriggerProcessingTimeMillis", "1000"));
        logger.info("query3dot2TriggerProcessingTimeMillis: " + query3dot2TriggerProcessingTimeMillis);
        enrichedParquetDataPath = prop.getProperty("enrichedParquetDataPath", "hdfs:///cs598ccc/parquet_data/enriched_ontimeperf");
        sparkLogLevel = prop.getProperty("sparkLogLevel","WARN");
        logger.info("sparkLogLevel: " + sparkLogLevel);


        if (input != null) {
            input.close();
        }

    }


    private void start() throws StreamingQueryException {

        SparkSession spark = SparkSession.builder()
                .appName("Group3StreamingProcessor")
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

        Dataset<Row> enriched_ontime_perf_df_leg1 = spark.read().format("parquet")
                .load(enrichedParquetDataPath)
                .where(col("Year").equalTo(2008));

        Dataset<Row> leg1 = enriched_ontime_perf_df_leg1
                .withColumnRenamed("id","Leg1_Id")
                .withColumnRenamed("Year", "Leg1_Year")
                .withColumnRenamed("Month","Leg1_Month")
                .withColumnRenamed("Origin","Leg1_Origin")
                .withColumnRenamed("Dest","Leg1_Dest")
                .withColumnRenamed("FlightDate","Leg1_FlightDate")
                .withColumnRenamed("DepTime", "Leg1_DepTime")
                .withColumnRenamed("ArrTime", "Leg1_ArrTime")
                .withColumnRenamed("Carrier","Leg1_Carrier")
                .withColumnRenamed("FlightNum", "Leg1_FlightNum")
                .withColumnRenamed("ArrDelay","Leg1_ArrDelay")
                .withColumnRenamed("timestamp", "leg1_timestamp")
               ;

        logger.info("leg1 count: " + leg1.count());

        logger.info("leg1 Schema");

        leg1.printSchema();

        Dataset<Row> kafka_input_leg2 = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("subscribe", enrichedData2008KafkaTopic.trim())
                .option("startingOffsets", startingOffsets)
                .option("minPartitions", enrichedData2008KafkaTopicMinPartitions)
                .option("maxOffsetsPerTrigger", enrichedData2008KafkaTopicMinmaxOffsetsPerTrigger)
                .option("kafkaConsumer.pollTimeoutMs",30000)
                .load();

        Dataset<Row> enriched_ontime_perf_df_leg2 = kafka_input_leg2.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
                .select(from_json(col("value"), SchemaCreator.createSchemaWithId()).as("data"), col("timestamp"))
                .select("data.*", "timestamp");

        enriched_ontime_perf_df_leg2.dropDuplicates("id");


        Dataset<Row> leg2 = enriched_ontime_perf_df_leg2
                .withColumnRenamed("id","Leg2_Id")
                .withColumnRenamed("Year", "Leg2_Year")
                .withColumnRenamed("Origin","Leg2_Origin")
                .withColumnRenamed("Dest","Leg2_Dest")
                .withColumnRenamed("FlightDate","Leg2_FlightDate")
                .withColumnRenamed("DepTime", "Leg2_DepTime")
                .withColumnRenamed("ArrTime", "Leg2_ArrTime")
                .withColumnRenamed("Carrier","Leg2_Carrier")
                .withColumnRenamed("FlightNum", "Leg2_FlightNum")
                .withColumnRenamed("ArrDelay","Leg2_ArrDelay")
                .withColumnRenamed("timestamp", "leg2_timestamp")
                ;

        logger.info("leg2 Schema");

        leg2.printSchema();


        String joinType_q3_2 = "inner";

        Column joinExpression_q3_2 = leg1.col("Leg1_Dest").equalTo(leg2.col("Leg2_Origin")) 
                .and(leg1.col("Leg1_Year").equalTo(leg2.col("Leg2_Year")))
                .and(leg1.col("Leg1_Year").equalTo(2008))
                .and(leg2.col("Leg2_FlightDate").equalTo(date_add(leg1.col("Leg1_FlightDate"),2)))
                .and(leg1.col("Leg1_DepTime").lt(1200))
                .and(leg2.col("Leg2_DepTime").gt(1200))
                ;


        Dataset<Row> multi_city_flight = leg2.join(leg1, joinExpression_q3_2, joinType_q3_2)
                .select(col("Leg2_Id").as("id"),col("Leg1_Month"),col("Leg1_Origin"), col("Leg1_Dest"), col("Leg1_Carrier"), col("Leg1_FlightNum"), col("Leg1_FlightDate"), col("Leg1_DepTime")
                        , col("Leg1_ArrTime"), col("Leg1_ArrDelay"),
                        col("Leg2_Origin"), col("Leg2_Dest"), col("Leg2_Carrier"), col("Leg2_FlightNum"), col("Leg2_FlightDate"), col("Leg2_DepTime")
                        , col("Leg2_ArrTime"), col("Leg2_ArrDelay")

                )
                .withColumn("totalTripDelayInMinutes", expr("(Leg1_ArrDelay+Leg2_ArrDelay)"))
                //.orderBy(asc("Leg1_Month"), asc("Leg1_Origin"), asc("Leg1_Dest"), asc("Leg2_Dest"),asc("Leg1_FlightDate"),asc("totalTripDelayInMinutes"))
                ;

        logger.info("multi_city_flight Schema");

        multi_city_flight.printSchema();

        StreamingQuery query3Dot2KafkaSink = multi_city_flight.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .outputMode("append")
                .queryName("query3dot2")
                .option("topic", query3dot2KafkaTopic.trim())
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("checkpointLocation", query3dot2CheckpointLocation)
                //.trigger(Trigger.ProcessingTime(query3dot2TriggerProcessingTimeMillis.intValue(), TimeUnit.MILLISECONDS))
                .start();



        logger.info("Query id for streaming to Kafka sink is: " + query3Dot2KafkaSink.id());

        logger.info("Query name for streaming to Kafka sink is: " + query3Dot2KafkaSink.name());

        logger.info("Streaming to Kafka sink. Status is: " +  query3Dot2KafkaSink.status());


        spark.streams().awaitAnyTermination();




    }




















    }
