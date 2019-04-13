package cs598ccc.task2;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class Group3ResultsFileOutputer {
    private static Logger logger = Logger.getLogger(Group3ResultsFileOutputer.class);

    private Properties prop = new Properties();
    private String master = null;
    private String startingOffsets = null;
    private String kafkaHost = null;
    private String query3dot2KafkaTopic = null;
    private Integer query3dot2KafkaTopicMinPartitions = null;
    private String query3dot2Path = null;
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

            Group3ResultsFileOutputer instance = new Group3ResultsFileOutputer();
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
        query3dot2KafkaTopicMinPartitions = Integer.valueOf(prop.getProperty("query3dot2KafkaTopicMinPartitions","1"));
        query3dot2KafkaTopic = prop.getProperty("query3dot2KafkaTopic", "query3dot2-ns-multipart");
        logger.info("query3dot2KafkaTopic: " + query3dot2KafkaTopic);
        query3dot2Path = prop.getProperty("query3dot2Path", "/home/markcb2/queryResults/task2/query3dot2");
        logger.info("query3dot2Path: " + query3dot2Path);
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
                .config("spark.driver.memory","6g")
                .getOrCreate();

        spark.sparkContext().setLogLevel(sparkLogLevel);


        logger.info("SparkSession Started.");

        Dataset<Row> kafka_input = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("subscribe", query3dot2KafkaTopic.trim())
                .option("startingOffsets", startingOffsets)
                .option("minPartitions", query3dot2KafkaTopicMinPartitions)
                .option("kafkaConsumer.pollTimeoutMs", 30000)
                .load();

        Dataset<Row> query3dot2 = kafka_input.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
                .select(from_json(col("value"), SchemaCreator.createQuery3dot2Schema()).as("data"), col("timestamp"))
                .select("data.*", "timestamp");

        query3dot2.dropDuplicates("id");


        logger.info("query3dot2 row count: " + query3dot2.count());

        logger.info("query3dot2 Schema");

        query3dot2.printSchema();

        Dataset<Row> multi_city_flight = query3dot2
                .orderBy(asc("Leg1_Month"), asc("Leg1_Origin"), asc("Leg1_Dest"), asc("Leg2_Dest"),asc("Leg1_FlightDate"),asc("totalTripDelayInMinutes"));



        WindowSpec windowSpec_3_2 = Window.partitionBy("Leg1_Month","Leg1_Origin","Leg1_Dest", "Leg2_Dest", "Leg1_FlightDate").orderBy(asc("totalTripDelayInMinutes"));

        Dataset<Row> multi_city_flight_ranked_df = multi_city_flight.withColumn("rank", rank().over(windowSpec_3_2))
                .where(col("rank").lt(2))
                .drop("rank")
                .orderBy(asc("Leg1_FlightDate"), asc("Leg1_Origin"), asc("Leg1_Dest"), asc("Leg2_Dest"))
                ;


        //multi_city_flight_ranked_df.show(1000);


        multi_city_flight_ranked_df.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save(query3dot2Path);

        logger.info("CSV File Output with " + multi_city_flight_ranked_df.count() + " records for Query Result for Group 3, Question 2 Has Been Written to Cassandra");


        Dataset<Row> multi_city_flight_query_results_df = multi_city_flight_ranked_df.select(col("Leg1_Origin"), col("Leg1_Dest"), col("Leg1_Carrier"),
                col("Leg1_FlightNum"), col("Leg1_FlightDate"),col("Leg1_ArrDelay"),col("Leg2_Dest"),col("Leg2_Carrier"), col("Leg2_FlightNum"),
                col("Leg2_FlightDate"), col("Leg2_ArrDelay"),col("totalTripDelayInMinutes"))
                .where(
                    col("Leg1_Origin").equalTo("BOS")
                    .and(col("Leg1_Dest").equalTo("ATL"))
                        .and(col("Leg2_Dest").equalTo("LAX"))
                            .and(col("Leg1_FlightDate").equalTo(to_date(lit("2008-04-03"))))
                        .or(

                                col("Leg1_Origin").equalTo("PHX")
                                        .and(col("Leg1_Dest").equalTo("JFK"))
                                        .and(col("Leg2_Dest").equalTo("MSP"))
                                        .and(col("Leg1_FlightDate").equalTo(to_date(lit("2008-09-07"))))
                        )
                            .or(
                                    col("Leg1_Origin").equalTo("DFW")
                                            .and(col("Leg1_Dest").equalTo("STL"))
                                            .and(col("Leg2_Dest").equalTo("ORD"))
                                            .and(col("Leg1_FlightDate").equalTo(to_date(lit("2008-01-24"))))
                            )
                            .or(
                                    col("Leg1_Origin").equalTo("LAX")
                                            .and(col("Leg1_Dest").equalTo("MIA"))
                                            .and(col("Leg2_Dest").equalTo("LAX"))
                                            .and(col("Leg1_FlightDate").equalTo(to_date(lit("2008-05-16"))))
                            )
                );



        logger.info("multi-city flight query results from Cassandra");
        multi_city_flight_query_results_df.show();






    }


    }
