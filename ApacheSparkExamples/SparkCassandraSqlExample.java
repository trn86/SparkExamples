package com.spark.cassandra.exmple;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkCassandraSqlExample implements Serializable {

	private static final long serialVersionUID = 1L;

	private String KEYSPACE = "mydb";
	private String TABLE = "employee";
	private String EMPID = "empid";
	private String AGE = "age";
	private String NAME = "name";
	private String DEPT = "dept";
	private String JOB_NAME = "Cassandra Job";

	public static void main(String[] args) {
		SparkCassandraSqlExample sparkObj = new SparkCassandraSqlExample();
		sparkObj.getAllEmployeeAndMapEncoder();
	}

	public SparkSession getJavaSparkSession() {
		SparkSession sparkSession = new SparkSession.Builder().master("local[*]").appName(JOB_NAME).getOrCreate();
		return sparkSession;
	}

	// Read all records from cassandra from keyspace mydb and table name employee
	// mapped to Encode
	public void readDataFromCassandraTable() {
		SparkSession sparkSession = getJavaSparkSession();

		Dataset<Row> employeeDataset = sparkSession.read().format("org.apache.spark.sql.cassandra")
				.options(new HashMap<String, String>() {

					private static final long serialVersionUID = 1L;
					{
						put("keyspace", KEYSPACE);
						put("table", TABLE);
					}
				}).load().select(col(EMPID), col(NAME), col(AGE), col(DEPT));
		employeeDataset.show(false);
	}

	// Read all records from cassandra from keyspace mydb and table name employee
	public void getAllEmployeeAndMapEncoder() {
		SparkSession sparkSession = getJavaSparkSession();

		Dataset<Employee> employeeDataset = sparkSession.read().format("org.apache.spark.sql.cassandra")
				.options(new HashMap<String, String>() {

					private static final long serialVersionUID = 1L;
					{
						put("keyspace", KEYSPACE);
						put("table", TABLE);
					}
				}).load().as(Encoders.bean(Employee.class));
		employeeDataset.show(false);
	}

	// Read all records from cassandra from keyspace mydb and table name employee
	public void getAllEmployeeAndFilterDepartment() {
		SparkSession sparkSession = getJavaSparkSession();

		Dataset<Row> employeeDataset = sparkSession.read().format("org.apache.spark.sql.cassandra")
				.options(new HashMap<String, String>() {

					private static final long serialVersionUID = 1L;
					{
						put("keyspace", KEYSPACE);
						put("table", TABLE);
					}
				}).load().select(col(EMPID), col(NAME), col(AGE), col(DEPT));
		Dataset<Row> filteredDataset = employeeDataset.filter(col(DEPT).equalTo("R & D"));
		filteredDataset.show(false);

	}

	// Read all records from cassandra from keyspace mydb and table name employee
	// and update the existing records deptwhere dept is R & D
	public void getAllEmployeeAndUpdateDepartment() {
		SparkSession sparkSession = getJavaSparkSession();

		Dataset<Row> employeeDataset = sparkSession.read().format("org.apache.spark.sql.cassandra")
				.options(new HashMap<String, String>() {

					private static final long serialVersionUID = 1L;
					{
						put("keyspace", KEYSPACE);
						put("table", TABLE);
					}
				}).load().select(col(EMPID), col(NAME), col(AGE), col(DEPT));
		Dataset<Row> filteredDataset = employeeDataset.filter(col(DEPT).equalTo("R & D"));

		// Change the Dept of interns
		Dataset<Row> finalDataSet = filteredDataset.withColumn(NAME, lit("Intern Pool"));
		finalDataSet.show(false);
		finalDataSet.write().mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("keyspace", KEYSPACE)
				.option("table", TABLE).save();
	}

}
