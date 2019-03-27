package cs598ccc.task1.group1;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.log4j.Logger;

public class ConvertEnrichedOntimePerf2CSV {

    private static Logger logger = Logger.getLogger(ConvertEnrichedOntimePerf2CSV.class);


    public static void main(String[] argss){
        logger.info("Starting ConvertEnrichedOntimePerf2CSV");
        ConvertEnrichedOntimePerf2CSV app = new ConvertEnrichedOntimePerf2CSV();
        app.start();
    }


    public void start() {

        SparkSession spark = SparkSession.builder()
                .appName("ConvertEnrichedOntimePerf2CSV")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");


        logger.info("Reading Parquet Files ");

        Dataset<Row> enriched_ontimeperf_df = spark.read().format("parquet").load("/Users/markberman/data/cs598ccc/parquet_data/enriched_ontimeperf");
        enriched_ontimeperf_df.printSchema();

        logger.info("Writing CSV File Output");

        enriched_ontimeperf_df
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .partitionBy("Year","Month")
                .save("/Users/markberman/data/cs598ccc/enriched_ontimeperf");

        logger.info("CSV File Output Has Been Written");



    }


}
