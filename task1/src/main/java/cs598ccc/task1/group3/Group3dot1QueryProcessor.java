package cs598ccc.task1.group3;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.desc;

public class Group3dot1QueryProcessor {

    private static Logger logger = Logger.getLogger(Group3dot1QueryProcessor.class);

    public static void main(String[] args){
        logger.info("Starting Group 3 Dot 1 Query");
        Group3dot1QueryProcessor app = new Group3dot1QueryProcessor();
        app.start();

    }


    public void start(){

        SparkSession spark = SparkSession.builder()
                .appName("CGroup 3 Dot 1 Query")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        logger.info("Reading Parquet Files");

        Dataset<Row> parquet_df = spark.read().format("parquet").load("hdfs:///cs598ccc/parquet_data/enriched_ontimeperf");
        parquet_df.show(7);
        parquet_df.printSchema();

        logger.info("The parquet dataframe has " + parquet_df.count() + " rows. and " + parquet_df.rdd().getNumPartitions() + " partitions " );

        logger.info("Starting Query 3.1");


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

        logger.info("Airport Popularity (Departures + Arrivals)");


        Dataset<Row> airportPpularity_df = origins_df.join(destinations_df, joinExpression, joinType)
                .select(col("origin").alias("airport"), col("departure").alias("departures"), col("arrival").alias("arrivals"))
                .withColumn("totalArrivalsAndDepartures", expr("(arrivals+departures)"))
                .withColumn("tempPartition",lit(1))
                .drop("departures")
                .drop("arrivals")
                .orderBy(desc("totalArrivalsAndDepartures"))
                ;

        WindowSpec windowSpec_3_1 = Window.partitionBy("tempPartition").orderBy(desc("totalArrivalsAndDepartures"));

        airportPpularity_df = airportPpularity_df.withColumn("rank", rank().over(windowSpec_3_1))
                .withColumn("group", lit(1))
                .drop("tempPartition")
                ;

        System.out.println("Airport popularity based on total departures plus arrivals");
        airportPpularity_df.show();

        logger.info("Saving airport popularity to hdfs:////cs598ccc/queryResults/group3Dot1");

        airportPpularity_df.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save("hdfs:///cs598ccc/queryResults/group3Dot1");

        logger.info("Finished Query 3.1");
    }
}
