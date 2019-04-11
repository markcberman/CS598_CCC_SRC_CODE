package cs598ccc.task2;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class Group1ResultsFileOutputer {

    private static Logger logger = Logger.getLogger(Group1ResultsFileOutputer.class);
    private Properties prop = new Properties();
    private String master = null;
    private String startingOffsets = null;
    private String kafkaHost = null;
    private String query1dot1KafkaTopic = null;
    private String query1dot2KafkaTopic = null;
    private String query1dot1Path = null;
    private String query1dot2Path = null;
    private Integer query1dot1KafkaTopicMinPartitions = null;
    private Integer query1dot1KafkaTopicMinmaxOffsetsPerTrigger = null;
    private Integer query1dot2KafkaTopicMinPartitions = null;
    private Integer query1dot2KafkaTopicMinmaxOffsetsPerTrigger = null;
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

            Group1ResultsFileOutputer instance = new Group1ResultsFileOutputer();
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

    public void loadProperties(String propertyFileName) throws IOException, NumberFormatException{

        InputStream input = new FileInputStream(propertyFileName);

        // load a properties file
        prop.load(input);
        master = prop.getProperty("master", "local[*]");
        startingOffsets = prop.getProperty("startingOffsets", "latest");
        logger.info("startingOffsets: " + startingOffsets);
        kafkaHost = prop.getProperty("kafkaHost", "localhost:6667");
        logger.info("kafkaHost: " + kafkaHost);
        query1dot1KafkaTopic = prop.getProperty("query1dot1KafkaTopic","query1dot1-multipart");
        logger.info("query1dot1KafkaTopic: " + query1dot1KafkaTopic);
        query1dot2KafkaTopic = prop.getProperty("query1dot2KafkaTopic","query1dot2-multipart");
        logger.info("query1dot2KafkaTopic: " + query1dot2KafkaTopic);
        query1dot1Path = prop.getProperty("query1dot1Path", "/home/markcb2/queryResults/task2/query1dot1");
        logger.info("query1dot1Path: " + query1dot1Path);
        query1dot2Path = prop.getProperty("query1dot2Path", "/home/markcb2/queryResults/task2/query1dot2");
        logger.info("query1dot2Path: " + query1dot2Path);
        query1dot1KafkaTopicMinPartitions = Integer.valueOf(prop.getProperty("query1dot1KafkaTopicMinPartitions","1"));
        logger.info("query1dot1KafkaTopicMinPartitions: " + query1dot1KafkaTopicMinPartitions);
        query1dot1KafkaTopicMinmaxOffsetsPerTrigger = Integer.valueOf(prop.getProperty("query1dot1KafkaTopicMinmaxOffsetsPerTrigger","340"));
        logger.info("query1dot1KafkaTopicMinmaxOffsetsPerTrigger: " + query1dot1KafkaTopicMinmaxOffsetsPerTrigger);
        query1dot2KafkaTopicMinPartitions = Integer.valueOf(prop.getProperty("query1dot2KafkaTopicMinPartitions","1"));
        logger.info("query1dot2KafkaTopicMinPartitions: " + query1dot2KafkaTopicMinPartitions);
        query1dot2KafkaTopicMinmaxOffsetsPerTrigger = Integer.valueOf(prop.getProperty("query1dot2KafkaTopicMinmaxOffsetsPerTrigger","50"));
        logger.info("query1dot2KafkaTopicMinmaxOffsetsPerTrigger: " + query1dot2KafkaTopicMinmaxOffsetsPerTrigger);
        sparkLogLevel = prop.getProperty("sparkLogLevel","WARN");
        logger.info("sparkLogLevel: " + sparkLogLevel);

    }
    private void start() throws StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("Group1ResultsFileOutputer")
                .master(master)
                .getOrCreate();


        spark.sparkContext().setLogLevel(sparkLogLevel);

        logger.info("SparkSession Started.");




        Dataset<Row> query1dot1KafkaSource  = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers",kafkaHost)
                .option("subscribe", query1dot1KafkaTopic.trim())
                .option("startingOffsets", startingOffsets)
                .option("minPartitions", query1dot1KafkaTopicMinPartitions)
                .load();



        Dataset<Row> query1dot1_all_data = query1dot1KafkaSource.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
                .select(from_json(col("value"),SchemaCreator.createQuery1dot1Schema()).as("data"),col("timestamp"))
                .select("data.*", "timestamp")
                .orderBy(desc("timestamp"))
                ;


        Dataset<Row> query1dot1_results = query1dot1_all_data.groupBy("origin")
                .agg(
                        max(col("totalArrivalsAndDepartures")).alias("totalArrivalsAndDepartures")
                )
                .orderBy(desc("totalArrivalsAndDepartures"))
                .limit(10)
                ;

        query1dot1_results.show();

        query1dot1_results.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save(query1dot1Path);


        Dataset<Row> query1dot2KafkaSource  = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers",kafkaHost)
                .option("subscribe", query1dot2KafkaTopic.trim())
                .option("startingOffsets", startingOffsets)
                .option("minPartitions", query1dot2KafkaTopicMinPartitions)
                .load();



        Dataset<Row> query1dot2_all_data = query1dot2KafkaSource.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
                .select(from_json(col("value"),SchemaCreator.createQuery1dot2Schema()).as("data"),col("timestamp"))
                .select("data.*", "timestamp")
                .orderBy(desc("timestamp"),asc("avg_time_added"))
                ;

        Dataset<Row> query1dot2_results = query1dot2_all_data.groupBy("Carrier")
                .agg(
                    first("avg_time_added").alias("avg_time_added")
                )
                .orderBy(asc("avg_time_added"))
                .limit(10)
                ;

        query1dot2_results.show();

        query1dot2_results.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save(query1dot2Path);



    }
}
