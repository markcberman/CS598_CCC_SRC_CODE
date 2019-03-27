package cs598ccc.task1.group3;


import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.log4j.Logger;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;


public class Group3dot2QueryProcessorV2 {

    private static Logger logger = Logger.getLogger(Group3dot2QueryProcessorV2.class);

    public static void main(String[] args) {
        logger.info("Starting Group 3.2 V2 Queries");
        Group3dot2QueryProcessorV2 app = new Group3dot2QueryProcessorV2();
        app.start();
    }

    public void start(){

        SparkSession spark = SparkSession.builder()
                .appName("CGroup 3 Dot 2 Query V2")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        logger.info("Reading Parquet Files");

        Dataset<Row> enriched_ontimeperf = spark.read().format("parquet").load("hdfs:///cs598ccc/parquet_data/enriched_ontimeperf");
        enriched_ontimeperf.show(7);
        enriched_ontimeperf.printSchema();

        logger.info("Starting Query 3.2");

        Dataset<Row> leg1 = enriched_ontimeperf
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


                ;


        Dataset<Row> leg2 = enriched_ontimeperf
                .withColumnRenamed("Year", "Leg2_Year")
                .withColumnRenamed("Origin","Leg2_Origin")
                .withColumnRenamed("Dest","Leg2_Dest")
                .withColumnRenamed("FlightDate","Leg2_FlightDate")
                .withColumnRenamed("DepTime", "Leg2_DepTime")
                .withColumnRenamed("ArrTime", "Leg2_ArrTime")
                .withColumnRenamed("Carrier","Leg2_Carrier")
                .withColumnRenamed("FlightNum", "Leg2_FlightNum")
                .withColumnRenamed("ArrDelay","Leg2_ArrDelay")
                ;

        String joinType_q3_2 = "inner";

        Column joinExpression_q3_2 = leg1.col("Leg1_Dest").equalTo(leg2.col("Leg2_Origin"))
                .and(leg1.col("Leg1_Year").equalTo(leg2.col("Leg2_Year")))
                .and(leg1.col("Leg1_Year").equalTo(2008))
                .and(leg2.col("Leg2_FlightDate").equalTo(date_add(leg1.col("Leg1_FlightDate"),2)))
                .and(leg1.col("Leg1_DepTime").lt(1200))
                .and(leg2.col("Leg2_DepTime").gt(1200))
                ;

        Dataset<Row> multi_city_flight = leg1.join(leg2, joinExpression_q3_2, joinType_q3_2)
                .select(col("Leg1_Month"),col("Leg1_Origin"), col("Leg1_Dest"), col("Leg1_Carrier"), col("Leg1_FlightNum"), col("Leg1_FlightDate"), col("Leg1_DepTime")
                        , col("Leg1_ArrTime"), col("Leg1_ArrDelay"),
                        col("Leg2_Origin"), col("Leg2_Dest"), col("Leg2_Carrier"), col("Leg2_FlightNum"), col("Leg2_FlightDate"), col("Leg2_DepTime")
                        , col("Leg2_ArrTime"), col("Leg2_ArrDelay")

                )
                .withColumn("totalTripDelayInMinutes", expr("(Leg1_ArrDelay+Leg2_ArrDelay)"))
                .orderBy(asc("Leg1_Month"), asc("Leg1_Origin"), asc("Leg1_Dest"), asc("Leg2_Dest"),asc("Leg1_FlightDate"),asc("totalTripDelayInMinutes"))
                ;

        logger.info("number of multi-city flights that meet the criteria for 2008 is: " + multi_city_flight.count());
        multi_city_flight.show(100);

        WindowSpec windowSpec_3_2 = Window.partitionBy("Leg1_Month","Leg1_Origin","Leg1_Dest", "Leg2_Dest", "Leg1_FlightDate").orderBy(asc("totalTripDelayInMinutes"));

        Dataset<Row> multi_city_flight_filtered_df = multi_city_flight.withColumn("rank", rank().over(windowSpec_3_2))
                .where(col("rank").lt(2))
                .drop("rank");

        multi_city_flight_filtered_df.show(200);

        logger.info("Saving query 3.2 multi-city flights filtered results as parquet file with  " + multi_city_flight_filtered_df.count() + " rows ");

        multi_city_flight_filtered_df.write()
                .format("parquet")
                .mode("overwrite")
                .partitionBy("Leg1_Month")
                .save("hdfs:///cs598ccc/queryResults/group3Dot2_filtered");

        logger.info("Finished Query 3.2");



    }
}



