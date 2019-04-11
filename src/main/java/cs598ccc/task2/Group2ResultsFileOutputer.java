package cs598ccc.task2;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;


public class Group2ResultsFileOutputer {

    private static Logger logger = Logger.getLogger(Group1ResultsFileOutputer.class);
    private Properties prop = new Properties();
    private String master = null;
    private String startingOffsets = null;
    private String kafkaHost = null;
    private String query2dot1KafkaTopic = null;
    private String query2dot2KafkaTopic = null;
    private String query2dot4KafkaTopic = null;
    private String query2dot1Path = null;
    private String query2dot2Path = null;
    private String query2dot4Path = null;
    private Integer query2dot1KafkaTopicMinPartitions = null;
    private Integer query2dot2KafkaTopicMinPartitions = null;
    private Integer query2dot4KafkaTopicMinPartitions = null;
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

            Group2ResultsFileOutputer instance = new Group2ResultsFileOutputer();
            instance.loadProperties(propertyFileName);
            instance.start();
        }
        catch(IOException ioe){
            logger.error(ExceptionUtils.getStackTrace(ioe));
        }
        catch(NumberFormatException nfe){
            logger.error(ExceptionUtils.getStackTrace(nfe));
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
        query2dot1KafkaTopic = prop.getProperty("query2dot1KafkaTopic","query2dot1-multipart");
        logger.info("query2dot1KafkaTopic: " + query2dot1KafkaTopic);
        query2dot2KafkaTopic = prop.getProperty("query2dot2KafkaTopic","query2dot2-multipart");
        logger.info("query2dot2KafkaTopic: " + query2dot2KafkaTopic);
        query2dot4KafkaTopic = prop.getProperty("query2dot4KafkaTopic","query2dot4-multipart");
        logger.info("query2dot4KafkaTopic: " + query2dot4KafkaTopic);
        query2dot1Path = prop.getProperty("query2dot1Path", "/home/markcb2/queryResults/task2/query2dot1");
        logger.info("query2dot1Path: " + query2dot1Path);
        query2dot2Path = prop.getProperty("query2dot2Path", "/home/markcb2/queryResults/task2/query2dot2");
        logger.info("query2dot2Path: " + query2dot2Path);
        query2dot4Path = prop.getProperty("query2dot4Path", "/home/markcb2/queryResults/task2/query2dot4");
        logger.info("query2dot4Path: " + query2dot4Path);
        query2dot1KafkaTopicMinPartitions = Integer.valueOf(prop.getProperty("query2dot1KafkaTopicMinPartitions","1"));
        logger.info("query2dot1KafkaTopicMinPartitions: " + query2dot1KafkaTopicMinPartitions);
        query2dot2KafkaTopicMinPartitions = Integer.valueOf(prop.getProperty("query2dot2KafkaTopicMinPartitions","1"));
        logger.info("query2dot2KafkaTopicMinPartitions: " + query2dot2KafkaTopicMinPartitions);
        query2dot4KafkaTopicMinPartitions = Integer.valueOf(prop.getProperty("query2dot4KafkaTopicMinPartitions","1"));
        logger.info("query2dot4KafkaTopicMinPartitions: " + query2dot4KafkaTopicMinPartitions);
        sparkLogLevel = prop.getProperty("sparkLogLevel","WARN");
        logger.info("sparkLogLevel: " + sparkLogLevel);


    }


    private void start()  {
        SparkSession spark = SparkSession.builder()
                .appName("Group2ResultsFileOutputer")
                .master(master)
                .getOrCreate();

        spark.sparkContext().setLogLevel(sparkLogLevel);


        logger.info("SparkSession Started.");


        Dataset<Row> query2dot1KafkaSource = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("subscribe", query2dot1KafkaTopic.trim())
                .option("startingOffsets", startingOffsets)
                .option("minPartitions", query2dot1KafkaTopicMinPartitions)
                .load();


        Dataset<Row> query2dot1_all_data = query2dot1KafkaSource.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
                .select(from_json(col("value"), SchemaCreator.createQuery2dot1Schema()).as("data"), col("timestamp"))
                .select("data.*", "timestamp")
                //.orderBy(desc("timestamp"));
                .orderBy(asc("Origin"),asc("Carrier"),desc("timestamp"));

        //query2dot1_all_data.show(1000);

        Dataset<Row> query2dot1_reduced_data = query2dot1_all_data.groupBy("Origin", "Carrier")
                .agg(
                       first("avgDepartureDelay").as("avgDepartureDelay")
                )
                .orderBy(asc("Origin"),asc("Carrier"));

        //query2dot1_reduced_data.show(200);

        WindowSpec windowSpec = Window.partitionBy("Origin").orderBy(asc("avgDepartureDelay"));

        Dataset<Row> query2_1_ranked_df = query2dot1_reduced_data.withColumn("rank", rank().over(windowSpec))
                .where(col("rank").lt(11))
                .drop("rank")
                ;

        query2_1_ranked_df.show(50);

        query2_1_ranked_df.orderBy("Origin")
                .coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save(query2dot1Path);



        Dataset<Row> query2dot2KafkaSource = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("subscribe", query2dot2KafkaTopic.trim())
                .option("startingOffsets", startingOffsets)
                .option("minPartitions", query2dot2KafkaTopicMinPartitions)
                .load();


        Dataset<Row> query2dot2_all_data = query2dot2KafkaSource.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
                .select(from_json(col("value"), SchemaCreator.createQuery2dot2Schema()).as("data"), col("timestamp"))
                .select("data.*", "timestamp")
                //.orderBy(desc("timestamp"));
                .orderBy(asc("Origin"),asc("Dest"),desc("timestamp"));

        //query2dot2_all_data.show(1000);

        Dataset<Row> query2dot2_reduced_data = query2dot2_all_data.groupBy("Origin", "Dest")
                .agg(
                        first("avgDepartureDelay").as("avgDepartureDelay")
                )
                .orderBy(asc("Origin"),asc("Dest"));

        //query2dot2_reduced_data.show(200);

        WindowSpec windowSpec_2_2 = Window.partitionBy("Origin").orderBy(asc("avgDepartureDelay"));

        Dataset<Row> query2_2_ranked_df = query2dot2_reduced_data.withColumn("rank", rank().over(windowSpec_2_2))
                .where(col("rank").lt(11))
                .drop("rank");

        query2_2_ranked_df.show(50);


        query2_2_ranked_df.orderBy("Origin")
                .coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save(query2dot2Path);



        Dataset<Row> query2dot4KafkaSource = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("subscribe", query2dot4KafkaTopic.trim())
                .option("startingOffsets", startingOffsets)
                .option("minPartitions", query2dot4KafkaTopicMinPartitions)
                .load();


        Dataset<Row> query2dot4_all_data = query2dot4KafkaSource.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
                .select(from_json(col("value"), SchemaCreator.createQuery2dot4Schema()).as("data"), col("timestamp"))
                .select("data.*", "timestamp")
                //.orderBy(desc("timestamp"));
                .orderBy(asc("Origin"),asc("Dest"),desc("timestamp"));

        //query2dot4_all_data.show(1000);


        Dataset<Row> query2dot4_reduced_data = query2dot4_all_data.groupBy("Origin", "Dest")
                .agg(
                        first("avgArrivalDelay").as("avgArrivalDelay")
                )
                .orderBy(asc("Origin"),asc("Dest"));

        query2dot4_reduced_data.show(20);


        query2dot4_reduced_data.orderBy(asc("Origin"),asc("Dest"))
                .coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save(query2dot4Path);


    }











    }
