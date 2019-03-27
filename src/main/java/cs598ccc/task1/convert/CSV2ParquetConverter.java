package cs598ccc.task1.convert;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.log4j.Logger;


public class CSV2ParquetConverter {

    private static Logger logger = Logger.getLogger(CSV2ParquetConverter.class);

    public static void main(String[] args){
        logger.info("Starting Converting CSV Files to Partitioned Parquet Files");
        CSV2ParquetConverter app = new CSV2ParquetConverter();
        app.start();

    }

    public void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Convert CSV to Parquet")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("sep",",")
                .option("dateFormat", "y-M-d")
                .option("nullValue","")
                //.load("hdfs:///cs598ccc/raw_data/*/*.csv");
                .load("/Users/markberman/data/cs598ccc/raw_data/*/*.csv");



        logger.info("Number of input rows read: " + df.count());


        //removing rows where CRSDeptime or DepTime or CRSArrTime or ArrTime are null
        Dataset<Row> filtered_df = df.where(col("CRSDepTime").isNotNull())
                .where(col("DepTime").isNotNull())
                .where(col("CRSArrTime").isNotNull())
                .where(col("ArrTime").isNotNull())
                .where(col("Origin").isNotNull())
                .where(col("Dest").isNotNull())
                ;

        logger.info("Number of rows after null values filtered outt: " + filtered_df.count());

        //adding and dropping columns to the dataframe

        logger.info("Dropping unnecessary columns");

        Dataset<Row> cleansed_df = filtered_df.withColumn("departure", lit(1))
                .withColumn("arrival", lit(1))
                .drop("DistanceGroup")
                .drop("Quarter")
                .drop("UniqueCarrier")
                .drop("TailNum")
                .drop("OriginCityName")
                .drop("OriginState")
                .drop("OriginStateFips")
                .drop("OriginStateName")
                .drop("OriginWac")
                .drop("DestCityName")
                .drop("DestState")
                .drop("DestStateFips")
                .drop("DestStateName")
                .drop("DestWac")
                .drop("CancellationCode")
                .drop("ArrTimeBlk")
                .drop("DepTimeBlk")
                .drop("DepartureDelayGroups")
                .drop("CRSElapsedTime")
                .drop("flights")
                .drop("CarrierDelay")
                .drop("WeatherDelay")
                .drop("NASDelay")
                .drop("SecurityDelay")
                .drop("LateAircraftDelay")
                .drop("FirstDepTime")
                .drop("TotalAddGTime")
                .drop("LongestAddTime")
                .drop("DivAirportLandings")
                .drop("DivReachedDest")
                .drop("DivActualElapsedTime")
                .drop("DivArrDelay")
                .drop("DivDistance")
                .drop("Div1Airport")
                .drop("Div1AirportID")
                .drop("LongestAddGTime")
                .drop("Div1WheelsOn")
                .drop("Div1TotalGTime")
                .drop("Div1LongestGTime")
                .drop("Div1WheelsOff")
                .drop("Div1TailNum")
                .drop("Div2Airport")
                .drop("Div2WheelsOn")
                .drop("Div2TotalGTime")
                .drop("Div2LongestGTime")
                .drop("Div2WheelsOff")
                .drop("Div2TailNum")
                .drop("ArrivalDelayGroups")
                .drop("_c55")
                .drop("_c75");

        logger.info("Casting numeric and date fields to the appropriate data type");

        cleansed_df = cleansed_df.withColumn("Year", col("Year").cast(DataTypes.IntegerType))
                .withColumn("Month", col("Month").cast(DataTypes.IntegerType))
                .withColumn("DayofMonth", col("DayofMonth").cast(DataTypes.IntegerType))
                .withColumn("DayOfWeek", col("DayOfWeek").cast(DataTypes.IntegerType))
                .withColumn("FlightDate", col("FlightDate").cast(DataTypes.DateType))
                .withColumn("CRSDepTime", col("CRSDepTime").cast(DataTypes.IntegerType))
                .withColumn("DepTime", col("DepTime").cast(DataTypes.IntegerType))
                .withColumn("DepDelay", col("DepDelay").cast(DataTypes.createDecimalType(10,2)))
                .withColumn("DepDelayMinutes",col("DepDelayMinutes").cast(DataTypes.createDecimalType(10,2)))
                .withColumn("DepDel15",col("DepDel15").cast(DataTypes.IntegerType))
                .withColumn("TaxiOut", col("TaxiOut").cast(DataTypes.createDecimalType(10,2)))
                .withColumn("WheelsOff",col("WheelsOff").cast(DataTypes.IntegerType))
                .withColumn("WheelsOn", col("WheelsOn").cast(DataTypes.IntegerType))
                .withColumn("TaxiIn", col("TaxiIn").cast(DataTypes.createDecimalType(10,2)))
                .withColumn("CRSArrTime", col("CRSArrTime").cast(DataTypes.IntegerType))
                .withColumn("ArrTime", col("ArrTime").cast(DataTypes.IntegerType))
                .withColumn("ArrDelay", col("ArrDelay").cast(DataTypes.createDecimalType(10,2)))
                .withColumn("ArrDelayMinutes", col("ArrDelayMinutes").cast(DataTypes.createDecimalType(10,2)))
                .withColumn("ArrDel15", col("ArrDel15").cast(DataTypes.IntegerType))
                .withColumn("Cancelled", col("Cancelled").cast(DataTypes.IntegerType))
                .withColumn("Diverted", col("Diverted").cast(DataTypes.IntegerType))
                .withColumn("ActualElapsedTime", col("ActualElapsedTime").cast(DataTypes.createDecimalType(10,2)))
                .withColumn("Distance", col("Distance").cast(DataTypes.createDecimalType(10,2)))
                ;





        //System.out.println("First 15 rows of cleansed data");
        //cleansed_df.show(15);


        System.out.println("Data Schema after cleansing");
        cleansed_df.printSchema();


        logger.info("Writing data to parquet format at hdfs:///cs598ccc/parquet_data/ontimeperf");


        cleansed_df.write()
                .format("parquet")
                .mode("overwrite")
                .partitionBy("Year")
                //.save("hdfs:///cs598ccc/parquet_data/ontimeperf");
                .save("/Users/markberman/data/cs598ccc/parquet_data/ontimeperf");

        logger.info("Finished writing parquet files");
    }

}
