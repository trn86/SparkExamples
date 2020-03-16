package com.spark.cassandra.exmple;
import java.io.Serializable;

public class Employee implements Serializable {

	private static long serialVersionId = 11L;
	
	private String name;
	private String empId;
	private int age;
	private String dept;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getEmpId() {
		return empId;
	}

	public void setEmpId(String empId) {
		this.empId = empId;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public String getDept() {
		return dept;
	}

	public void setDept(String dept) {
		this.dept = dept;
	}

}
