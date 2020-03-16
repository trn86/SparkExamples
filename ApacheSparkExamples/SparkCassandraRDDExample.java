package com.spark.cassandra.exmple;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;

public class SparkCassandraRDDExample implements Serializable {

	private static final long serialVersionUID = 1L;

	private String KEYSPACE = "mydb";
	private String TABLE = "employee";
	private String EMPID = "empid";
	private String AGE = "age";
	private String NAME = "name";
	private String DEPT = "dept";
	private String JOB_NAME = "Cassandra Job";

	public static void main(String[] args) {
		SparkCassandraRDDExample sparkObj = new SparkCassandraRDDExample();
		sparkObj.getAllEmployeeAndUpdateDepartment();
	}

	public JavaSparkContext getJavaSparkContext() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(JOB_NAME);
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		return sparkContext;
	}

	// Read all records from cassandra from keyspace mydb and table name employee
	public void readDataFromCassandraTable() {
		JavaSparkContext sparkContext = getJavaSparkContext();
		JavaRDD<CassandraRow> cassandrarow = CassandraJavaUtil.javaFunctions(sparkContext)
				.cassandraTable(KEYSPACE, TABLE)
				.select(CassandraJavaUtil.column(EMPID), CassandraJavaUtil.column(NAME), CassandraJavaUtil.column(AGE));

		JavaRDD<String> rdd = cassandrarow.map(x -> "Emp Id : + " + x.getString(EMPID) + " Emp Name : "
				+ x.getString(NAME) + " AGE : " + x.getInt(AGE));
		
		// Print all records
		rdd.collect().forEach(x -> System.out.println(x));
	}

	// Read all records from cassandra from keyspace mydb and table name employee
	public void getAllEmployeeAndUpdateDepartment() {
		JavaSparkContext sparkContext = getJavaSparkContext();
		JavaRDD<CassandraRow> cassandrarow = CassandraJavaUtil.javaFunctions(sparkContext)
				.cassandraTable(KEYSPACE, TABLE).select(CassandraJavaUtil.column(EMPID), CassandraJavaUtil.column(NAME),
						CassandraJavaUtil.column(DEPT), CassandraJavaUtil.column(AGE));

		// Filter rdd where dept is equals R & D
		JavaRDD<CassandraRow> filteredRdd = cassandrarow.filter(x -> x.getString(DEPT).equals("R & D"));
		JavaRDD<Employee> finalRdd = filteredRdd.map(x -> {
			Employee emp = new Employee();
			emp.setEmpId(x.getString(EMPID));
			emp.setName(x.getString(NAME));
			emp.setAge(x.getInt(AGE));
			emp.setDept("Interns");
			return emp;
		});

		// Map fields
		Map<String, String> schemaMap = new HashMap<String, String>();
		schemaMap.put(EMPID, EMPID);
		schemaMap.put(NAME, NAME);
		schemaMap.put(AGE, AGE);
		schemaMap.put(DEPT, DEPT);

		// Write data to cassandra
		CassandraJavaUtil.javaFunctions(finalRdd)
				.writerBuilder(KEYSPACE, TABLE, CassandraJavaUtil.mapToRow(Employee.class, schemaMap))
				.saveToCassandra();

	}

}
