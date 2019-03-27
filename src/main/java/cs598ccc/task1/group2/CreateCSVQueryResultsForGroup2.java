package cs598ccc.task1.group2;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateCSVQueryResultsForGroup2 {

    private static Logger logger = Logger.getLogger(Group2QueryProcessor.class);

    public static void main(String[] args) {
        logger.info("Starting Creation of CSV Files for Group 2 Query Results");
        CreateCSVQueryResultsForGroup2 app = new CreateCSVQueryResultsForGroup2();
        app.start();
    }

    public void start(){

        SparkSession spark = SparkSession.builder()
                .appName("CSV Creation For Group 2 Query Results")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");


        logger.info("Reading Parquet Files for Query Result for Group 2, Question 1");

        Dataset<Row> query2Dot1FilteredQueryResults_df = spark.read().format("parquet").load("hdfs:///cs598ccc/queryResults/group2Dot1_filtered");
        query2Dot1FilteredQueryResults_df.show(7);
        query2Dot1FilteredQueryResults_df.printSchema();

        logger.info("Reading Parquet Files for Query Result for Group 2, Question 2");

        Dataset<Row> query2Dot2FilteredQueryResults_df = spark.read().format("parquet").load("hdfs:///cs598ccc/queryResults/group2Dot2_filtered");
        query2Dot2FilteredQueryResults_df.show(7);
        query2Dot2FilteredQueryResults_df.printSchema();

        logger.info("Reading Parquet Files for Query Result for Group 2, Question 4");

        Dataset<Row> query2Dot4QueryResults_df = spark.read().format("parquet").load("hdfs:///cs598ccc/queryResults/group2Dot4");
        query2Dot4QueryResults_df.show(7);
        query2Dot4QueryResults_df.printSchema();

        logger.info("Writing CSV File Output for Query Result for Group 2, Question 1");

        query2Dot1FilteredQueryResults_df.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save("hdfs:///cs598ccc/queryResults/group2Dot1_filtered_csv");

        logger.info("CSV File Output for Query Result for Group 2, Question 1 Has Been Written");

        logger.info("Writing CSV File Output for Query Result for Group 2, Question 2");

        query2Dot2FilteredQueryResults_df.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save("hdfs:///cs598ccc/queryResults/group2Dot2_filtered_csv");

        logger.info("CSV File Output for Query Result for Group 2, Question 2 Has Been Written");

        logger.info("Writing CSV File Output for Query Result for Group 2, Question 4");

        query2Dot4QueryResults_df.coalesce(1)
                .write()
                .format("csv")
                .mode("overwrite")
                .option("sep", ",")
                .option("header", "true")
                .save("hdfs:///cs598ccc/queryResults/group2Dot4_csv");

        logger.info("CSV File Output for Query Result for Group 2, Question 4 Has Been Written");

    }
}
