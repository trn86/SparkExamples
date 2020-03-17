package com.spark.mysql.example;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkMySqlSqlExample implements Serializable {

	private static final long serialVersionUID = 1L;

	private String DB_URL = "jdbc:mysql://localhost:3306/mydb";
	private String DB_DRIVER = "com.mysql.jdbc.Driver";
	private String DB_UNAME = "root";
	private String DB_UPASSWORD = "Wh@tever123";
	private String TABLE = "employee";
	private String ID = "id";
	private String NAME = "name";
	private String DEPT = "dept";
	private String JOB_NAME = "MySql Job";

	public static void main(String[] args) {
		SparkMySqlSqlExample sparkObj = new SparkMySqlSqlExample();
		sparkObj.readDataFromMySqlTable();
	}

	private Map<String, String> getDbDetails() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("url", DB_URL);
		map.put("driver", DB_DRIVER);
		map.put("dbtable", TABLE);
		map.put("user", DB_UNAME);
		map.put("password", DB_UPASSWORD);
		return map;
	}

	public SparkSession getJavaSparkSession() {
		SparkSession sparkSession = new SparkSession.Builder().master("local[*]").appName(JOB_NAME).getOrCreate();
		return sparkSession;
	}

	// Read all records from mysql db and table name employee
	// mapped to Encoder
	public void readDataFromMySqlTable() {
		SparkSession sparkSession = getJavaSparkSession();

		Dataset<Row> employeeDataset = sparkSession.read().format("jdbc").options(getDbDetails()).load()
				.select(col("id"), col("name"), col("dept"));
		employeeDataset.show(false);
	}

	// Read all records from mydb and table name employee
	public void getAllEmployeeAndMapEncoder() {
		SparkSession sparkSession = getJavaSparkSession();

		Dataset<MySqlEmployee> employeeDataset = sparkSession.read().format("jdbc").options(getDbDetails()).load()
				.as(Encoders.bean(MySqlEmployee.class));
		employeeDataset.show(false);
	}

	// Read all records from mydb and table name employee
	public void getAllEmployeeAndFilterDepartment() {
		SparkSession sparkSession = getJavaSparkSession();

		Dataset<Row> employeeDataset = sparkSession.read().format("jdbc").options(getDbDetails()).load();
		Dataset<Row> filteredDataset = employeeDataset.filter(col(DEPT).equalTo("Technology"));
		filteredDataset.show(false);

	}

	// Read all records from mysql mydb and table name employee
	// and update the existing records dept where dept is Technology
	public void getAllEmployeeAndUpdateDepartment() {
		SparkSession sparkSession = getJavaSparkSession();

		Dataset<Row> employeeDataset = sparkSession.read().format("jdbc").options(getDbDetails()).load().select(col(ID),
				col(NAME), col(DEPT), col("salary"));
		Dataset<Row> filteredDataset = employeeDataset.filter(col(DEPT).equalTo("Technology"));

		// Change the Dept of interns
		Dataset<Row> finalDataSet = filteredDataset.withColumn("dept", lit("Tech"));
		finalDataSet.show(false);
		finalDataSet.write().mode(SaveMode.Append).format("jdbc").options(new HashMap<String, String>() {

			private static final long serialVersionUID = 1L;
			{
				put("url", DB_URL);
				put("driver", "com.mysql.jdbc.Driver");
				put("dbtable", TABLE);
				put("user", "root");
				put("password", "Wh@tever123");
			}
		}).save();
	}

}
