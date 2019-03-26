package package cs598ccc.task2;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SchemaCreator {

	
	public static StructType createSchema(){
		StructType mySchema = DataTypes.createStructType(new StructField[]{ //#A
                DataTypes.createStructField(
                        "Month", //#B
                        DataTypes.IntegerType, //#C
                        true), //#D
                DataTypes.createStructField(
                        "DayofMonth",
                        DataTypes.IntegerType,
                        true),
                DataTypes.createStructField(
                        "DayOfWeek",
                        DataTypes.IntegerType,
                        true),
                DataTypes.createStructField(
                        "FlightDate",
                        DataTypes.DateType,
                        true),
                DataTypes.createStructField(
                        "AirlineID",
                        DataTypes.StringType,
                        true),
                DataTypes.createStructField(
                        "Carrier",
                        DataTypes.StringType,
                        true),
                DataTypes.createStructField(
                        "FlightNum",
                        DataTypes.StringType,
                        true),
                DataTypes.createStructField(
                        "Origin",
                        DataTypes.StringType,
                        true), 
                DataTypes.createStructField(
                        "Dest",
                        DataTypes.StringType,
                        true), 
                DataTypes.createStructField(
                        "CRSDepTime",
                        DataTypes.IntegerType,
                        true), 
                DataTypes.createStructField(
                        "DepTime",
                        DataTypes.IntegerType,
                        true), 
                DataTypes.createStructField(
                        "DepDelay",
                        DataTypes.createDecimalType(10,2),
                        true), 
                DataTypes.createStructField(
                        "DepDelayMinutes",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "DepDel15",
                        DataTypes.IntegerType,
                        true), 
                DataTypes.createStructField(
                        "TaxiOut",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "WheelsOff",
                        DataTypes.IntegerType,
                        true), 
                DataTypes.createStructField(
                        "WheelsOn",
                        DataTypes.IntegerType,
                        true), 
                DataTypes.createStructField(
                        "TaxiIn",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "CRSArrTime",
                        DataTypes.IntegerType,
                        true), 
                DataTypes.createStructField(
                        "ArrTime",
                        DataTypes.IntegerType,
                        true), 
                DataTypes.createStructField(
                        "ArrDelay",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "ArrDelayMinutes",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "ArrDel15",
                        DataTypes.IntegerType,
                        true), 
                DataTypes.createStructField(
                        "Cancelled",
                        DataTypes.IntegerType,
                        true), 
                DataTypes.createStructField(
                        "Diverted",
                        DataTypes.IntegerType,
                        true), 
                DataTypes.createStructField(
                        "ActualElapsedTime",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "AirTime",
                        DataTypes.StringType,
                        true),
                DataTypes.createStructField(
                        "Distance",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "departure",
                        DataTypes.IntegerType,
                        true), 
                DataTypes.createStructField(
                        "arrival",
                        DataTypes.IntegerType,
                        true), 
                DataTypes.createStructField(
                        "Year",
                        DataTypes.IntegerType,
                        false)});
		return mySchema;
	}

}
