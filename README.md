# SparkExamples

This is all code for spark rdd and spark sql examples.

To work with spark rdd,you require spark-core_2.11 ,add this to you maven dependency:

<!-- Required for using spark rdd !-->
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-core_2.11</artifactId>
			<version>2.4.0</version>
		</dependency>


To work with spark rdd,you require spark-sql_2.11 ,add this to you maven dependency:
<!-- Required for using spark sql !-->
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-sql_2.11</artifactId>
			<version>2.4.0</version>
		</dependency>

RDD has 2 operations

Transformations (Are evaluated lazily) :
Map -> Does a change of object from one type to another
MapPartitions -> Does change for all partitions of object from one type to another
Reduce -> Reduce is to combine values in same partition 
ReduceByKey - Combines common keys data in the partitions and moves data to the node where same keys should reside (Causes less shuffle)
GroupBykey -> Shuffles data and moves data to the node where same keys should reside (Causes more shuffle)
Join - To join one or more rdd data
Subtract - To return the records,which are not present or common from left side of rdd 
LeftOuerJoin - To return the records,which are present/common in both rdds and from left side of rdd 

Actions (Are eagerly evaluated)  : 

Collect - Get all output
ForEach - Do something for each element
Count - Count total rdd objects
First - Returns first rdd object
Take - taken n no of rdd objects as specified in take(n)

The same operations are available for Spark Sql as well.

Sql Actions :

show() -> To display data in tabular format
show(false) -> To display data in tabular format with formatted output
show(n) -> To display n no of data in tabular format
count() -> returns count

Sql Transformations :

select -> select no no of columns specified
groupBy -> aggregate operation based on column specified

com.spark.rdd.example -> Contains Spark RDD Example
com.spark.sql.example -> Contains Spark Sql Example
com.spark.cassandra.exmple -> Constains Spark RDD and Spark Sql example for connecting,reading,updating data with Spark Sql and with Datastax Cassandra Connector
com.my.spark.context -> Create spark context and spark session

To work with cassandra examples,you need to install apache cassadndra datatabse and python to connect to cqlsh (the cassandra client).
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


