package cs598ccc.task2;

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
                        "origin_airport_code",
                        DataTypes.StringType,
                        true),
                DataTypes.createStructField(
                        "departureDelay",
                        DataTypes.createDecimalType(14,6),
                        true),
                DataTypes.createStructField(
                        "lon_origin",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "lat_origin",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "dest_airport_code",
                        DataTypes.StringType,
                        true),
                DataTypes.createStructField(
                        "arrivalDelay",
                        DataTypes.createDecimalType(14,6),
                        true),
                DataTypes.createStructField(
                        "lon_dest",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "lat_dest",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "Year",
                        DataTypes.IntegerType,
                        false)});
        return mySchema;
    }

    public static StructType createSchemaWithId(){
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
                        "origin_airport_code",
                        DataTypes.StringType,
                        true),
                DataTypes.createStructField(
                        "departureDelay",
                        DataTypes.createDecimalType(14,6),
                        true),
                DataTypes.createStructField(
                        "lon_origin",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "lat_origin",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "dest_airport_code",
                        DataTypes.StringType,
                        true),
                DataTypes.createStructField(
                        "arrivalDelay",
                        DataTypes.createDecimalType(14,6),
                        true),
                DataTypes.createStructField(
                        "lon_dest",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "lat_dest",
                        DataTypes.createDecimalType(10,2),
                        true),
                DataTypes.createStructField(
                        "Year",
                        DataTypes.IntegerType,
                        true),
                DataTypes.createStructField(
                        "id",
                        DataTypes.LongType,
                        false)
        });

        return mySchema;


    }

    public static StructType createQuery1dot1Schema(){
        StructType mySchema = DataTypes.createStructType(new StructField[]{ //#A
                DataTypes.createStructField(
                        "origin",
                        DataTypes.StringType,
                        true),
                DataTypes.createStructField(
                        "totalArrivalsAndDepartures",
                        DataTypes.IntegerType,
                        false)

        });

        return mySchema;

    }


    public static StructType createQuery1dot2Schema(){
        StructType mySchema = DataTypes.createStructType(new StructField[]{ //#A
                DataTypes.createStructField(
                        "group",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "Carrier",
                        DataTypes.StringType,
                        true),
                DataTypes.createStructField(
                        "avg_time_added",
                        DataTypes.createDecimalType(14,6),
                        false)

        });

        return mySchema;

    }


    public static StructType createQuery2dot1Schema(){
        StructType mySchema = DataTypes.createStructType(new StructField[]{ //#A
                DataTypes.createStructField(
                        "Origin",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "Carrier",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "avgDepartureDelay",
                        DataTypes.createDecimalType(14,6),
                        false)

        });

        return mySchema;

    }

    public static StructType createQuery2dot2Schema(){
        StructType mySchema = DataTypes.createStructType(new StructField[]{ //#A
                DataTypes.createStructField(
                        "Origin",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "Dest",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "avgDepartureDelay",
                        DataTypes.createDecimalType(14,6),
                        false)

        });

        return mySchema;

    }



    public static StructType createQuery2dot4Schema(){
        StructType mySchema = DataTypes.createStructType(new StructField[]{ //#A
                DataTypes.createStructField(
                        "Origin",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "Dest",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "avgArrivalDelay",
                        DataTypes.createDecimalType(14,6),
                        false)

        });

        return mySchema;

    }

    public static StructType createQuery3dot2Schema(){
        StructType mySchema = DataTypes.createStructType(new StructField[]{ //#A
                DataTypes.createStructField(
                        "id",
                        DataTypes.LongType,
                        false),
                DataTypes.createStructField(
                        "leg1_timestamp",
                        DataTypes.TimestampType,
                        false),
                DataTypes.createStructField(
                        "Leg1_Month",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "Leg1_Origin",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "Leg1_Dest",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "Leg1_Carrier",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "Leg1_FlightNum",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "Leg1_FlightDate",
                        DataTypes.DateType,
                        false),
                DataTypes.createStructField(
                        "Leg1_DepTime",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "Leg1_ArrTime",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "Leg1_ArrDelay",
                        DataTypes.createDecimalType(10,2),
                        false),
                DataTypes.createStructField(
                        "leg2_timestamp",
                        DataTypes.TimestampType,
                        false),
                DataTypes.createStructField(
                        "Leg2_Origin",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "Leg2_Dest",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "Leg2_Carrier",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "Leg2_FlightNum",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "Leg2_FlightDate",
                        DataTypes.DateType,
                        false),
                DataTypes.createStructField(
                        "Leg2_DepTime",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "Leg2_ArrTime",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "Leg2_ArrDelay",
                        DataTypes.createDecimalType(10,2),
                        false),
                DataTypes.createStructField(
                        "totalTripDelayInMinutes",
                        DataTypes.createDecimalType(10,2),
                        false)

        });

        return mySchema;

    }




    public static void main(String[] args){
        System.out.println(SchemaCreator.createSchema());
        System.out.println(SchemaCreator.createSchemaWithId());
        System.out.println(SchemaCreator.createQuery1dot1Schema());
        System.out.println(SchemaCreator.createQuery1dot2Schema());
        System.out.println(SchemaCreator.createQuery2dot1Schema());
        System.out.println(SchemaCreator.createQuery2dot2Schema());
        System.out.println(SchemaCreator.createQuery2dot4Schema());
        System.out.println(SchemaCreator.createQuery3dot2Schema());

    }

}

