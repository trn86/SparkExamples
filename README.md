## Spark Rdd and DataFrame/Spark Sql Examples using MySql and Cassandra with Java

This is all code for spark rdd and spark sql examples.

## Prerequisite
Spark ,Cassandra , MySql and Java 8 or higher

## Dependencies
#### Spark
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-core_2.11</artifactId>
			<version>2.4.0</version>
		</dependency>

		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-sql_2.11</artifactId>
			<version>2.4.0</version>
		</dependency>

#### Cassandra
		<dependency>
			<groupId>com.datastax.spark</groupId>
			<artifactId>spark-cassandra-connector_2.11</artifactId>
			<version>2.4.0</version>
		</dependency>
		
#### MySql
		<dependency>
			<groupId>mysql</groupId>
			<artifactId>mysql-connector-java</artifactId>
			<version>8.0.19</version>
		</dependency>

RDD has 2 operations

## Transformations (Operations Evaluated Lazily) :
Map -> Does a change of object from one type to another
MapPartitions -> Does change for all partitions of object from one type to another
Reduce -> Reduce is to combine values in same partition 
ReduceByKey - Combines common keys data in the partitions and moves data to the node where same keys should reside (Causes less shuffle)
GroupBykey -> Shuffles data and moves data to the node where same keys should reside (Causes more shuffle)
Join - To join one or more rdd data
Subtract - To return the records,which are not present or common from left side of rdd 
LeftOuerJoin - To return the records,which are present/common in both rdds and from left side of rdd 

## Actions (Operations evaluated eagerly)  : 

Collect - Get all output
ForEach - Do something for each element
Count - Count total rdd objects
First - Returns first rdd object
Take - taken n no of rdd objects as specified in take(n)

The same operations are available for Spark Sql as well.

## Sql Actions

show() -> To display data in tabular format
show(false) -> To display data in tabular format with formatted output
show(n) -> To display n no of data in tabular format
count() -> returns count

## Sql Transformations

select -> select no no of columns specified
groupBy -> aggregate operation based on column specified

com.spark.rdd.example -> Contains Spark RDD Example
com.spark.sql.example -> Contains Spark Sql Example
com.spark.cassandra.exmple -> Constains Spark RDD and Spark Sql example for connecting,reading,updating data with Spark Sql and with Datastax Cassandra Connector
com.my.spark.context -> Create spark context and spark session



## Cassandra Steps To Be Done

To work with cassandra examples,you need to install apache cassandra datatabse and python to connect to cqlsh (the cassandra client).
You need to created the keyspace (database) and then the table.Follow these steps in cqlsh :

CREATE KEYSPACE mydb WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

use mydb;

CREATE TABLE  employee(
    empId varchar,
    name varchar,
    age int,
    dept varchar,
    PRIMARY KEY(empId)
);

insert into employee (empId,name,age,dept) values ('1','Tarun',31,'R & D');
insert into employee (empId,name,age,dept) values ('2','Jeetu',39,'R & D');
insert into employee (empId,name,age,dept) values ('3','Tamana',20,'R & D-Intern');
insert into employee (empId,name,age,dept) values ('4','Bheem',33,'Admin');
insert into employee (empId,name,age,dept) values ('5','Ashish',48,'Security');
insert into employee (empId,name,age,dept) values ('6','Neesha',27,'HR');
insert into employee (empId,name,age,dept) values ('7','TarunV',30,'Finance');


## My Sql Steps To Be Done

CREATE DATABASE myDatabase;

mysql -u root -p 
mypassword


create database mydb;

use mydb;

create table mydb.employee (
id INT AUTO_INCREMENT PRIMARY KEY,
name varchar(20),
dept varchar(10),
salary int(10)
);

insert into mydb.employee values(100,'Thomas','Sales',5000);
insert into mydb.employee values(200,'Jason','Technology',5500);
insert into mydb.employee values(300,'Mayla','Technology',7000);
insert into mydb.employee values(400,'Nisha','Marketing',9500);
insert into mydb.employee values(500,'Randy','Technology',6000);

