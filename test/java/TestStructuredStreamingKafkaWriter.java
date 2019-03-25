
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.streaming.Trigger;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.concurrent.TimeUnit;


public class TestStructuredStreamingKafkaWriter {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName("Spark-Kafka-Integration-part1")
                .master("local[*]")
                .getOrCreate();


        StructType mySchema = DataTypes.createStructType(new StructField[]{ //#A
                DataTypes.createStructField(
                        "id", //#B
                        DataTypes.IntegerType, //#C
                        false), //#D
                DataTypes.createStructField(
                        "name",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "year",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "rating",
                        DataTypes.DoubleType,
                        false),
                DataTypes.createStructField(
                        "duration",
                        DataTypes.IntegerType,
                        false)});


        Dataset<Row> input_df = spark.readStream().schema(mySchema)
                .option("maxFilesPerTrigger", 12)
                .csv("/Users/markberman/data/test");

        StreamingQuery query =  input_df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").
                writeStream()
                .queryName("movie-data")
                .format("kafka")
                .option("topic", "test-full-lifecycle-multipart")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("checkpointLocation", "/Users/markberman/data/checkpoint/TestStructuredStreamingKafkaWriter")
                .trigger(Trigger.ProcessingTime(3000, TimeUnit.MILLISECONDS))
                .start();

        query.explain();

        System.out.println(query.lastProgress());

        query.awaitTermination();
    }

}
