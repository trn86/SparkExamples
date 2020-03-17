package com.spark.mysql.example;

import java.io.Serializable;

import com.spark.cassandra.exmple.Employee;

public class MySqlEmployee implements Serializable {

	private static long serialVersionId = 11L;

	private String id;
	private String name;
	private String dept;
	private int salary;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDept() {
		return dept;
	}

	public void setDept(String dept) {
		this.dept = dept;
	}

	public int getSalary() {
		return salary;
	}

	public void setSalary(int salary) {
		this.salary = salary;
	}

}
