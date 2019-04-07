package cs598ccc.task2;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class Setup {

    private static Logger logger = Logger.getLogger(Setup.class);
    private Properties prop = new Properties();
    private String master = null;
    private String enrichedParquetDataPath = null;
    private String originAirportsPath = null;
    private String destAirportsPath = null;
    private String enrichedOntimePerf2Task2Subset = null;
    private String enrichedOntimePerf2Task2SubsetParquet = null;


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

            Setup instance = new Setup();
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
        enrichedParquetDataPath = prop.getProperty("enrichedParquetDataPath", "hdfs:///cs598ccc/parquet_data/ontimeperf");
        logger.info("enrichedParquetDataPath: " + enrichedParquetDataPath);
        originAirportsPath = prop.getProperty("originAirportsPath", "hdfs:///cs598ccc/ref_data/origin_airports");
        logger.info("originAirportsPath: " + originAirportsPath);
        destAirportsPath = prop.getProperty("destAirportsPath","hdfs:///cs598ccc/ref_data/dest_airports");
        logger.info("destAirportsPath: " + destAirportsPath);
        enrichedOntimePerf2Task2Subset = prop.getProperty("enrichedOntimePerf2Task2Subset", "hdfs:///cs598ccc/csv_data/task2/enriched_ontimeperf_task2_subset");
        logger.info("enrichedOntimePerf2Task2Subset: " + enrichedOntimePerf2Task2Subset);
        enrichedOntimePerf2Task2SubsetParquet = prop.getProperty("enrichedOntimePerf2Task2SubsetParquet", "hdfs:///cs598ccc/parquet_data/task2/enriched_ontimeperf_task2_subset");





        if (input != null) {
            input.close();
        }

    }

    public void start() throws StreamingQueryException {

        SparkSession spark = SparkSession.builder()
                .appName("Setup")
                .master(master)
                .getOrCreate();

        logger.info("SparkSession Started.");



        logger.info("Reading Parquet Files");

        Dataset<Row> enriched_ontime_perf_batch_df = spark.read().format("parquet")
                .load(enrichedParquetDataPath);
        enriched_ontime_perf_batch_df.printSchema();

        Dataset<Row> origin_airports_df = enriched_ontime_perf_batch_df
                .select(col("origin")).distinct()
                .withColumnRenamed("origin","origin_airport_code");

        logger.info("Number of Distinct Origin Airports: " + origin_airports_df.count());

        origin_airports_df.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save(originAirportsPath);

        Dataset<Row> dest_airports_df = enriched_ontime_perf_batch_df
                .select(col("dest")).distinct()
                .withColumnRenamed("dest","dest_airport_code");
        logger.info("Number of Distinct Destination Airports: " + dest_airports_df.count());

        dest_airports_df.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save(destAirportsPath);


        Dataset<Row> enriched_ontime_perf_task2_subset_df = enriched_ontime_perf_batch_df
                .withColumn("id", functions.hash(enriched_ontime_perf_batch_df.col("Year")
                        ,enriched_ontime_perf_batch_df.col("Month")
                        , enriched_ontime_perf_batch_df.col("DayofMonth")
                        ,enriched_ontime_perf_batch_df.col("DepTime")
                        ,enriched_ontime_perf_batch_df.col("AirlineID")
                        ,enriched_ontime_perf_batch_df.col("FlightNum")
                        )
                )
                .where(
                        col("Year").equalTo("2008")
                        .and(
                            col("Origin").equalTo("BOS")
                            .and(col("Dest").equalTo("ATL")
                                    .and(col("Month").equalTo(4))
                            )
                            .or(
                                col("Origin").equalTo("ATL")
                                .and(col("Dest").equalTo("LAX"))
                                    .and(col("Month").equalTo(4))
                            )
                            .or(
                                col("Origin").equalTo("PHX")
                                        .and(col("Dest").equalTo("JFK"))
                                        .and(col("Month").equalTo(9))
                            ).or(
                                col("Origin").equalTo("JFK")
                                        .and(col("Dest").equalTo("MSP"))
                                        .and(col("Month").equalTo(9))
                            ).or(
                                col("Origin").equalTo("DFW")
                                        .and(col("Dest").equalTo("STL"))
                                        .and(col("Month").equalTo(1))
                            ).or(
                                col("Origin").equalTo("STL")
                                        .and(col("Dest").equalTo("ORD"))
                                        .and(col("Month").equalTo(1))
                            ).or(
                                col("Origin").equalTo("LAX")
                                        .and(col("Dest").equalTo("MIA"))
                                        .and(col("Month").equalTo(5))
                            ).or(
                                col("Origin").equalTo("MIA")
                                        .and(col("Dest").equalTo("LAX"))
                                        .and(col("Month").equalTo(5))
                            )
                        )

                );

        enriched_ontime_perf_task2_subset_df.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save(enrichedOntimePerf2Task2Subset);



        enriched_ontime_perf_task2_subset_df.coalesce(1)
                .write()
                .format("parquet")
                .mode("overwrite")
                .save(enrichedOntimePerf2Task2SubsetParquet);

    }

}
