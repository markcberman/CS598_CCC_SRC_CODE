package cs598ccc.task1.test.log4j;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class Log4jTester {

    private static Logger logger = Logger.getLogger(Log4jTester.class);

    public static void main(String[] args){
        logger.info("Writing FIRST TEST log message to log file");
        Log4jTester app = new Log4jTester();
        app.start();

    }

    public void start() {

        logger.info("Writing Another TEST log message to log file");

        SparkSession spark = SparkSession.builder()
                .appName("Log4j Tester")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");






    }










}
