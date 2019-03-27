package cs598ccc.task1.group1;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.time.Year;


public class PrintSchema {

        private static Logger logger = Logger.getLogger(PrintSchema.class);

        public static void main(String[] args){
            logger.info("Sarting PrintSchema");
            cs598ccc.task1.group1.PrintSchema app = new cs598ccc.task1.group1.PrintSchema();
            app.start();

        }

        public void start() {
            SparkSession spark = SparkSession.builder()
                    .appName("Print Schema")
                    .master("local[*]")
                    .getOrCreate();

            spark.sparkContext().setLogLevel("WARN");

            logger.info("Reading enriched_ontimeperf parquet files");

            Dataset<Row> parquet_df = spark.read().format("parquet")
                    .load("/Users/markberman/data/cs598ccc/parquet_data/enriched_ontimeperf");
            parquet_df = parquet_df
                //.withColumn("id", functions.monotonically_increasing_id());
                .withColumn("id", functions.hash(parquet_df.col("Year"),parquet_df.col("Month"), parquet_df.col("DayofMonth"),parquet_df.col("DepTime"),parquet_df.col("AirlineID"),parquet_df.col("FlightNum")));
            parquet_df.show(100);
            logger.info(parquet_df.count());
            parquet_df.printSchema();
        }


}
