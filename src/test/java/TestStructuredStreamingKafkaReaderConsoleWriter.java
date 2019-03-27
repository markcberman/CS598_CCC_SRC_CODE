import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.streaming.StreamingQuery;


public class TestStructuredStreamingKafkaReaderConsoleWriter {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark-Kafka-Integration-2")
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

        Dataset<Row> kafka_input = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test-full-lifecycle-multipart")
                .option("startingOffsets", "earliest")
                .option("minPartitions", 4)
                .option("maxOffsetsPerTrigger", 1000)
                .option("startingOffsets", "earliest")
                .load();


        Dataset<Row> jsonified_data = kafka_input.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
                .select(from_json(col("value"),mySchema).as("data"),col("timestamp"))
                .select("data.*", "timestamp");

        jsonified_data.printSchema();

        StreamingQuery console_sink = jsonified_data
                .writeStream()
                .queryName("console_movie_data")
                .format("console")
                .option("truncate","false")
                .start();


        console_sink.awaitTermination();
    }

}
