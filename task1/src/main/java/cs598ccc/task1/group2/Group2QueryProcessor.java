package cs598ccc.task1.group2;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.log4j.Logger;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class Group2QueryProcessor {

    private static Logger logger = Logger.getLogger(Group2QueryProcessor.class);

    public static void main(String[] args) {
        logger.info("Starting Group 2 Queries");
        Group2QueryProcessor app = new Group2QueryProcessor();
        app.start();
    }


    public void start(){

        SparkSession spark = SparkSession.builder()
                .appName("CGroup 2 Queries")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");


        logger.info("Reading Parquet Files");

        Dataset<Row> enriched_ontime_perf_df = spark.read().format("parquet").load("hdfs:///cs598ccc/parquet_data/enriched_ontimeperf");
        enriched_ontime_perf_df.show(7);
        enriched_ontime_perf_df.printSchema();



        logger.info("The enriched_ontime_perf_df dataframe has " + enriched_ontime_perf_df.count() + " rows. and " + enriched_ontime_perf_df.rdd().getNumPartitions() + " partitions " );

        enriched_ontime_perf_df.write()
                .format("parquet")
                .mode("overwrite")
                .partitionBy("Year")
                .saveAsTable("enriched_ontimeperf");


        Dataset<Row> orign_airports_df = spark.read().format("parquet").load("hdfs:///cs598ccc/parquet_data/origin_airports");
        logger.info("Number of Distinct Origin Airports: " + orign_airports_df.count());

        orign_airports_df.write()
                .format("parquet")
                .mode("overwrite")
                .saveAsTable("orign_airports");


        Dataset<Row> dest_airports_df = spark.read().format("parquet").load("hdfs:///cs598ccc/parquet_data/dest_airports");
        logger.info("Number of Distinct Destination Airports: " + dest_airports_df.count());

        dest_airports_df.write()
                .format("parquet")
                .mode("overwrite")
                .saveAsTable("dest_airports");

        logger.info("Starting query 2.1");

        spark.catalog().listTables().show();



        Dataset<Row> query2_1_unfiltered_results_df = spark.sql("SELECT Origin, Carrier, avg(DepDelay) as avgDepartureDelay FROM enriched_ontimeperf WHERE Origin in (SELECT origin_airport_code FROM orign_airports) GROUP By Origin, Carrier  ORDER BY Origin ASC, avgDepartureDelay ASC");
        //query2_1_unfiltered_results_df.show(200);

        logger.info("Saving query 2.1 unfiltered results as parquet file with  " + query2_1_unfiltered_results_df.count() + " rows ");

        query2_1_unfiltered_results_df.write()
                .format("parquet")
                .mode("overwrite")
                .partitionBy("Origin")
                .save("hdfs:///cs598ccc/queryResults/group2Dot1_unfiltered");

        WindowSpec windowSpec = Window.partitionBy("Origin").orderBy(asc("avgDepartureDelay"));

        Dataset<Row> query2_1_filtered_df = query2_1_unfiltered_results_df.withColumn("rank", rank().over(windowSpec))
                .where(col("rank").lt(11))
                .drop("rank");

        query2_1_filtered_df.show(200);

        logger.info("Saving query 2.1 filtered results as parquet file with  " + query2_1_filtered_df.count() + " rows ");

        query2_1_filtered_df.write()
                .format("parquet")
                .mode("overwrite")
                .partitionBy("Origin")
                .save("hdfs:///cs598ccc/queryResults/group2Dot1_filtered");

        logger.info("Finished Group 2: Question 1");


        logger.info("Starting query 2.2");


       Dataset<Row> query2_2_unfiltered_results_df = spark.sql("SELECT Origin, Dest, avg(DepDelay) as avgDepartureDelay FROM enriched_ontimeperf WHERE Origin in (SELECT dest_airport_code FROM dest_airports) GROUP By Origin, Dest  ORDER BY Origin ASC, avgDepartureDelay ASC");
        query2_2_unfiltered_results_df.show(200);


        logger.info("Saving query 2.2 unfiltered results as parquet file with  " + query2_2_unfiltered_results_df.count() + " rows ");

        query2_2_unfiltered_results_df.write()
                .format("parquet")
                .mode("overwrite")
                .partitionBy("Origin")
                .save("hdfs:///cs598ccc/queryResults/group2Dot2_unfiltered");

        WindowSpec windowSpec_2_2 = Window.partitionBy("Origin").orderBy(asc("avgDepartureDelay"));

        Dataset<Row> query2_2_filtered_df = query2_2_unfiltered_results_df.withColumn("rank", rank().over(windowSpec_2_2))
                .where(col("rank").lt(11))
                .drop("rank");

        query2_2_filtered_df.show(200);

        logger.info("Saving query 2.2 filtered results as parquet file with  " + query2_2_filtered_df.count() + " rows ");

        query2_2_filtered_df.write()
                .format("parquet")
                .mode("overwrite")
                .partitionBy("Origin")
                .save("hdfs:///cs598ccc/queryResults/group2Dot2_filtered");

        logger.info("Finished Group 2: Question 2");

        logger.info("starting query 2.3");


        Dataset<Row> query2_4_results_df = spark.sql("SELECT Origin, Dest, avg(ArrDelay) as avgArrivalDelay FROM enriched_ontimeperf GROUP By Origin, Dest  ORDER BY Origin ASC, Dest ASC");
        query2_4_results_df.show(200);

        logger.info("Saving query 2.4 results as parquet file with  " + query2_4_results_df.count() + " rows ");

        query2_4_results_df.write()
                .format("parquet")
                .mode("overwrite")
                .partitionBy("Origin")
                .save("hdfs:///cs598ccc/queryResults/group2Dot4");

        logger.info("Finished Group 2: Question 4");


    }
}
