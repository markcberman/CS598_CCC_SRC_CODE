package cs598ccc.task1.group1;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.log4j.Logger;

public class DataEnrichmentProcessor {

    private static Logger logger = Logger.getLogger(DataEnrichmentProcessor.class);

    public static void main(String[] argss){
        logger.info("Starting DataEnrichmentProcessor");
        DataEnrichmentProcessor app = new DataEnrichmentProcessor();
        app.start();
    }

    public void start(){

        SparkSession spark = SparkSession.builder()
                .appName("Raw Data Enrichment")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");


        logger.info("Reading Parquet Files");

        Dataset<Row> parquet_df = spark.read().format("parquet")
                .load("/Users/markberman/data/cs598ccc/parquet_data/ontimeperf");
                //.load("hdfs:///cs598ccc/parquet_data/ontimeperf");
        parquet_df.show(7);
        parquet_df.printSchema();

        logger.info("The parquet dataframe has " + parquet_df.count() + " rows. and " + parquet_df.rdd().getNumPartitions() + " partitions " );

        Dataset<Row> origin_airports_df = parquet_df.groupBy("origin")
                .agg(
                        avg(col("DepDelay")).alias("departureDelay")
                )
                .orderBy(asc("origin"));

        logger.info("Distinct Origin Domestic Airport Average Departure Delays: " + origin_airports_df.count());

        origin_airports_df.show(400);


        origin_airports_df.write().format("parquet").saveAsTable("origin_airports");


        Dataset<Row> dest_airports_df = parquet_df.groupBy("dest")
                .agg(
                        avg(col("ArrDelay")).alias("arrivalDelay")
                )
                .orderBy(asc("dest"));

        logger.info("Distinct Arrival Domestic Airport Arrival Delays: " + dest_airports_df.count());

        dest_airports_df.show(400);

        dest_airports_df.write().format("parquet").saveAsTable("dest_airports");


        logger.info("Reading master coordinate data.");

        Dataset<Row> masterCoordintes_df = spark.read().format("csv")
                .option("header", "true")
                .option("sep",",")
                .option("dateFormat", "y-M-d")
                .option("nullValue","")
                //.load("hdfs:///cs598ccc/ref_data/417923300_T_MASTER_CORD_All_All.csv");
                .load("/Users/markberman/data/cs598ccc/ref_data/417923300_T_MASTER_CORD_All_All.csv");

        logger.info("Number of master coordinate data input rows read: " + masterCoordintes_df.count());


        Dataset<Row> usa_airport_long_lat_df = masterCoordintes_df
                .select(col("AIRPORT").alias("AIRPORT"), col("TR_COUNTRY_NAME").alias("COUNTRY"),col("LONGITUDE").alias("LONGITUDE"), col("LATITUDE").alias("LATITUDE"), col("END_DATE"))
                .distinct()
                .where(col("TR_COUNTRY_NAME").equalTo("United States of America"))
                .where(col("END_DATE").isNull())
                .withColumn("LONGITUDE",col("LONGITUDE").cast(DataTypes.createDecimalType(10,2)))
                .withColumn("LATITUDE",col("LATITUDE").cast(DataTypes.createDecimalType(10,2)))
                .orderBy(asc("AIRPORT"))
                ;

        logger.info("Distinct USA airport long-lat records returned: " + usa_airport_long_lat_df.count());

        //usa_airport_long_lat_df.show(8000);

        usa_airport_long_lat_df.write().format("parquet").saveAsTable("airport_long_lat");

        Dataset<Row> usa_origin_airport_lon_lat_df = spark.sql("SELECT DISTINCT AIRPORT, LONGITUDE, LATITUDE FROM  airport_long_lat WHERE AIRPORT in (SELECT origin FROM origin_airports) ORDER BY AIRPORT ASC");

        logger.info("Distinct usa_origin_airport_lon_lat_df records: " +  usa_origin_airport_lon_lat_df.count());

        //usa_origin_airport_lon_lat_df.show(400);

        Dataset<Row> usa_dest_airport_lon_lat_df = spark.sql("SELECT DISTINCT AIRPORT, LONGITUDE, LATITUDE FROM  airport_long_lat WHERE AIRPORT in (SELECT dest FROM dest_airports) ORDER BY AIRPORT ASC");

        logger.info("Distinct usa_dest_airport_lon_lat_df records: " +  usa_dest_airport_lon_lat_df.count());

        //usa_dest_airport_lon_lat_df.show(400);

        logger.info("Comparision between origin airport lists");

        String joinType_Left_Outer = "left_outer";

        Column joinExpression_origin_airport = origin_airports_df.col("origin").equalTo(usa_origin_airport_lon_lat_df.col("AIRPORT"));

        Dataset<Row> orign_airports_avg_delay_and_gps_df = origin_airports_df.join(usa_origin_airport_lon_lat_df,joinExpression_origin_airport,joinType_Left_Outer)
                .where(col("LONGITUDE").isNotNull())
                .withColumnRenamed("origin","origin_airport_code")
                .drop("AIRPORT")
                .orderBy(desc("origin_airport_code"))
                ;

        logger.info("Origin Airports With Average Delay and GPS Record Count: " + orign_airports_avg_delay_and_gps_df.count());

        orign_airports_avg_delay_and_gps_df.show(10);

        orign_airports_avg_delay_and_gps_df.write().format("parquet").saveAsTable("orign_airports_avg_departure_delay_and_gps");


        Column joinExpression_dest_airport = dest_airports_df.col("dest").equalTo(usa_dest_airport_lon_lat_df.col("AIRPORT"));

        Dataset<Row> dest_airports_avg_delay_and_gps_df = dest_airports_df.join(usa_dest_airport_lon_lat_df,joinExpression_dest_airport,joinType_Left_Outer)
                .where(col("LONGITUDE").isNotNull())
                .withColumnRenamed("dest","dest_airport_code")
                .drop("AIRPORT")
                .orderBy(desc("dest_airport_code"))
                ;

        logger.info("Dest Airports With Average Delay and GPS Record Count: " + dest_airports_avg_delay_and_gps_df.count());

        dest_airports_avg_delay_and_gps_df.show(10);

        dest_airports_avg_delay_and_gps_df.write().format("parquet").saveAsTable("dest_airports_avg_arrival_delay_and_gps");

        spark.catalog().listTables().show();



        logger.info("Performing inner Join of parguet_df and orign_airports_avg_delay_and_gps_df by origin airport code in order to get long and lat and average departure delay for each origin airport");
        Column joinExpression_origin = parquet_df.col("origin").equalTo(orign_airports_avg_delay_and_gps_df.col("origin_airport_code"));

        String joinType_inner = "inner";

        Dataset<Row> enriched_on_time_perf_temp_df = parquet_df.join(orign_airports_avg_delay_and_gps_df,joinExpression_origin,joinType_inner)
                .withColumn("LONGITUDE",col("LONGITUDE").cast(DataTypes.createDecimalType(10,2)))
                .withColumnRenamed("LONGITUDE","lon_origin")
                .withColumn("LATITUDE",col("LATITUDE").cast(DataTypes.createDecimalType(10,2)))
                .withColumnRenamed("LATITUDE","lat_origin")
                .orderBy(desc("origin"))
                ;


        logger.info("Number of rows in enriched_on_time_perf_temp_df is: " + enriched_on_time_perf_temp_df.count());

        //enriched_on_time_perf_temp_df.show(50);

        logger.info("Performing inner Join of parguet_df and dest_airports_avg_delay_and_gps_df by dest airport code in order to get long and lat and average arrival delay for each destination airport ");
        Column joinExpression_dest = enriched_on_time_perf_temp_df.col("dest").equalTo(dest_airports_avg_delay_and_gps_df.col("dest_airport_code"));

        Dataset<Row> enriched_on_time_perf_final_df = enriched_on_time_perf_temp_df.join(dest_airports_avg_delay_and_gps_df,joinExpression_dest,joinType_inner)
                .withColumn("LONGITUDE",col("LONGITUDE").cast(DataTypes.createDecimalType(10,2)))
                .withColumnRenamed("LONGITUDE","lon_dest")
                .withColumn("LATITUDE",col("LATITUDE").cast(DataTypes.createDecimalType(10,2)))
                .withColumnRenamed("LATITUDE","lat_dest")
        ;

        logger.info("Number of rows in enriched_on_time_perf_final_df is: " + enriched_on_time_perf_final_df.count());

        //enriched_on_time_perf_final_df.show(50);

        logger.info("Writing data to parquet format at hdfs://hdfs:///cs598ccc/parquet_data/enriched_ontimeperf");

        enriched_on_time_perf_final_df
                .repartition(21)
                .write()
                .format("parquet")
                .mode("overwrite")
                .partitionBy("Year")
                //.save("hdfs:///cs598ccc/parquet_data/enriched_ontimeperf");
                .save("/Users/markberman/data/cs598ccc/parquet_data/enriched_ontimeperf");

        logger.info("Finished writing parquet files");



    }

}
