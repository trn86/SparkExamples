package com.spark.sql.example;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.my.spark.context.SparkSessionContext;

/**
 * All operations with Spark sql using struct field to define coluns,header and data type for each column of the csv
 *
 */
public class SparkSqlWithCustomStructExample {

	public static void main(String[] args) {
		String path = "src/main/resources/data.csv";
		SparkSession session = SparkSessionContext.getSessionSparkContext().getSession();

		// Creater fields witg data type
		StructType mySchema = new StructType(
				new StructField[] { new StructField("No", DataTypes.IntegerType, true, Metadata.empty()),
						new StructField("Id", DataTypes.IntegerType, true, Metadata.empty()),
						new StructField("Name", DataTypes.StringType, true, Metadata.empty()),
						new StructField("Age", DataTypes.IntegerType, true, Metadata.empty()),
						new StructField("Photo", DataTypes.StringType, true, Metadata.empty()),
						new StructField("Nationality", DataTypes.StringType, true, Metadata.empty()),
						new StructField("Flag", DataTypes.StringType, true, Metadata.empty()),
						new StructField("Club", DataTypes.StringType, true, Metadata.empty()),
						new StructField("Value", DataTypes.StringType, true, Metadata.empty()),
						new StructField("Wage", DataTypes.StringType, true, Metadata.empty()),
						new StructField("Joined", DataTypes.StringType, true, Metadata.empty()),
						new StructField("Contract Valid Until", DataTypes.StringType, true, Metadata.empty()),
						new StructField("Release Clause", DataTypes.StringType, true, Metadata.empty()) });
		// To get records from csv and add header,as first row of the records
		Dataset<Row> df = session.read().schema(mySchema).option("header", true).csv(path);

		// Show 50 records with no formatting as false
		df.show(50, false);

		// Select 50 names
		df.select(col("Name")).show(false);
	}

}