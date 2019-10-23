package com.spark.sql.example;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.my.spark.context.SparkSessionContext;

/**
 * All operations with Spark sql
 *
 */
public class SparkSqlExample {

	public static void main(String[] args) {
		String path = "src/main/resources/data.csv";
		SparkSession session = SparkSessionContext.getSessionSparkContext().getSession();

		// To get records from csv and add header,as first row of the records
		Dataset<Row> df = session.read().option("inferSchema", false).option("header", true).csv(path);

		// Show 50 records with no formatting as false,show is a action
		df.show(50, false);

		// Create temp view
		df.createOrReplaceTempView("players");
		session.sql("SELECT Name,Age,Nationality FROM players where Nationality = 'Portugal'").show(false);

		// Count the users with same name,groupBy is a transformation
		df.groupBy(col("Name")).count().show(false);

		// Show namee of players from only Nationality Portugal,filter is a
		// transformation
		df.filter(col("Nationality").equalTo("Portugal")).show(false);

		// Select column name,age and nationality of players for only Nationality
		// Portugal records,select is a transformation
		df.select(col("Name"), col("Age"), col("Nationality")).filter(col("Nationality").equalTo("Portugal"))
				.show(false);

	}

}