package com.usbank.edm.test;

import org.apache.spark.sql.SparkSession;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TestSparkPropertyLoading {
	public static void main(String[] args){
		
		 String propertyFileName = null;
		 Properties prop = new Properties();
		 InputStream input = null;
		 String sparkMasterLocation = null;
		 String applicationName = null;
		 String inputTopicName = null;
		 String outputTopicName = null;
		 String kafkaHost = null;
		 

		 if (args.length != 1) {
		      System.out.println("Fully Qualified Path and File Name <path>/<fileName> is missing from the command line for the configuration file. Please specify ");
		      System.exit(-1);
		    } else {
		    	propertyFileName = args[0];
		      System.out.println("configuration file path and file name: " + propertyFileName);
		    }
		    
		 try {

				input = new FileInputStream(propertyFileName);

				// load a properties file
				prop.load(input);
				sparkMasterLocation = prop.getProperty("sparkMasterLocation");
				applicationName = prop.getProperty("appName");
				kafkaHost = prop.getProperty("kafkaHost");
				inputTopicName = prop.getProperty("inputKafkaTopic");
				outputTopicName = prop.getProperty("outputKafkaTopic");
				// get the property value and print it out
				System.out.println(sparkMasterLocation);
				System.out.println(applicationName);
				System.out.println(kafkaHost);
				System.out.println(inputTopicName);
				System.out.println(outputTopicName);
				

			} catch (IOException ex) {
				ex.printStackTrace();
			} finally {
				if (input != null) {
					try {
						input.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

		 SparkSession spark = SparkSession
	                .builder()
	                .appName(applicationName)
	                .master(sparkMasterLocation)
	                .getOrCreate();
		 
		 System.out.println("**** Started Spark Session ******");
	}
}
