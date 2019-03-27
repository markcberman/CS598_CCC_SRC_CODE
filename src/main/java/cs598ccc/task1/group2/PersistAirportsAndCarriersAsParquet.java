package cs598ccc.task1.group2;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class PersistAirportsAndCarriersAsParquet
{

    private static Logger logger = Logger.getLogger(PersistAirportsAndCarriersAsParquet.class);

    public static void main(String[] args) {
        logger.info("Starting Persistence of Airports and Carriers as Parquet File Based Reference Data");
        PersistAirportsAndCarriersAsParquet app = new PersistAirportsAndCarriersAsParquet();
        app.start();
    }

    public void start(){
        SparkSession spark = SparkSession.builder()
                .appName("Persist Airports and Carriers As Parquet Files")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");


        logger.info("Reading Parquet Files");

        Dataset<Row> enriched_ontime_perf_df = spark.read().format("parquet").load("hdfs:///cs598ccc/parquet_data/enriched_ontimeperf");
        enriched_ontime_perf_df.show(7);
        enriched_ontime_perf_df.printSchema();

        Dataset<Row> origin_airports_df = enriched_ontime_perf_df.select(col("origin")).distinct()
                .withColumnRenamed("origin","origin_airport_code");
        logger.info("Number of Distinct Origin Airports: " + origin_airports_df.count());

        origin_airports_df.write()
                .format("parquet")
                .mode("overwrite")
                .save("hdfs:///cs598ccc/parquet_data/origin_airports");


        Dataset<Row> dest_airports_df = enriched_ontime_perf_df.select(col("dest")).distinct()
                .withColumnRenamed("dest","dest_airport_code");
        logger.info("Number of Distinct Destination Airports: " + dest_airports_df.count());

        dest_airports_df.write()
                .format("parquet")
                .mode("overwrite")
                .save("hdfs:///cs598ccc/parquet_data/dest_airports");

        Dataset<Row> carriers_df = enriched_ontime_perf_df.select(col("Carrier")).distinct()
                .withColumnRenamed("Carrier", "Airline");
        logger.info("Number of Distinct Airlines: " + carriers_df.count());

        carriers_df.write()
                .format("parquet")
                .mode("overwrite")
                .save("hdfs:///cs598ccc/parquet_data/airlines");


    }
}
