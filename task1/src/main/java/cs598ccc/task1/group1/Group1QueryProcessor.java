package cs598ccc.task1.group1;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.log4j.Logger;

public class Group1QueryProcessor {
    private static Logger logger = Logger.getLogger(Group1QueryProcessor.class);

    public static void main(String[] args){
        logger.info("Starting Group 1 Queries");
        Group1QueryProcessor app = new Group1QueryProcessor();
        app.start();

    }

    public void start() {
        SparkSession spark = SparkSession.builder()
                .appName("CGroup 1 Queries")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        logger.info("Reading Parquet Files");

        Dataset<Row> parquet_df = spark.read().format("parquet").load("hdfs:///cs598ccc/parquet_data/enriched_ontimeperf");
        parquet_df.show(7);
        parquet_df.printSchema();

        logger.info("The parquet dataframe has " + parquet_df.count() + " rows. and " + parquet_df.rdd().getNumPartitions() + " partitions " );



        logger.info("Starting Group 1: Question 1");


        Dataset<Row> groupedby_df = parquet_df.groupBy("origin", "dest")
                .agg(
                        sum(col("departure")).alias("departure"),
                        sum(col("arrival")).alias("arrival")
                )
                .orderBy(asc("origin"), asc("dest"));

        //groupedby_df.show(1000);

        Dataset<Row> origins_df = parquet_df.groupBy("origin")
                .agg(
                        sum(col("departure")).alias("departure")
                )
                .orderBy(asc("origin"));
        //origins_df.show(1000);

        logger.info("Number of unique origin airports: " + origins_df.count());


        Dataset<Row> destinations_df = parquet_df.groupBy("dest")
                .agg(
                        sum(col("arrival")).alias("arrival")
                )
                .orderBy(asc("dest"));

        //destinations_df.show(1000);

        logger.info("Number of unique destination airports: " + destinations_df.count());

        Column joinExpression = origins_df.col("origin").equalTo(destinations_df.col("dest"));

        String joinType = "inner";

        logger.info("Querying for Top 10 Airports (Departures + Arrivals)");

        Dataset<Row> topTenPopularAirports_df = origins_df.join(destinations_df, joinExpression, joinType)
                .select(col("origin").alias("airport"), col("departure").alias("departures"), col("arrival").alias("arrivals"))
                .withColumn("group", lit(1))
                .withColumn("totalArrivalsAndDepartures", expr("(arrivals+departures)"))
                .drop("departures")
                .drop("arrivals")
                .orderBy(desc("totalArrivalsAndDepartures"))
                .limit(10)
                ;

        System.out.println("Airport popularity based on total departures plus arrivals");
        topTenPopularAirports_df.show();

        logger.info("Saving top 10 airports to hdfs:///cs598ccc/queryResults/group1Dot1");

        topTenPopularAirports_df.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save("hdfs:///cs598ccc/queryResults/group1Dot1");



        logger.info("Finished Group 1: Question 1");


        logger.info("Starting Group 1: Question 2");

        Dataset<Row> airline_on_time_arrival_performance = parquet_df
                .withColumn("group", lit(1))
                .withColumnRenamed("departureDelay", "avgOriginDelay")
                .withColumnRenamed("arrivalDelay", "avgDestDelay")
                .withColumn("totalTripDelay", expr("(DepDelay+ArrDelay)"))
                .withColumn("typicalTripTime", expr("((0.117*Distance) +(0.517 * (lon_origin - lon_dest)) + 42.3) + avgOriginDelay + avgDestDelay"))
                .withColumn("time_added", expr(("ActualElapsedTime + totalTripDelay - typicalTripTime")))
                //.orderBy(asc("Carrier"), asc("origin"), asc("dest"), desc("FlightDate"), asc("DepTime"))
                ;
        //airline_on_time_arrival_performance.show(10);


        Dataset<Row> topTenAirlineOnTimeArrivalPerformance_df = airline_on_time_arrival_performance.groupBy("group","Carrier")
                .agg(
                        avg(col("time_added")).alias("avg_time_added")
                )
                .orderBy(asc("avg_time_added"))
                .limit(10);

        topTenAirlineOnTimeArrivalPerformance_df.show();

        topTenAirlineOnTimeArrivalPerformance_df.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save("hdfs:///cs598ccc/queryResults/group1Dot2");

        logger.info("Finished Group 1: Question 2");
    }



}
