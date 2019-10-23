package com.spark.rdd.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.my.spark.context.SparkContext;

/**
 * Program to sum all nos using reduce operation
 *
 */
public class ReduceExample {

	public static void main(String[] args) {
		SparkContext sc = SparkContext.getSparkContext();
		List<Integer> myFriends = new ArrayList<Integer>();
		myFriends.add(1);
		myFriends.add(2);
		myFriends.add(3);
		myFriends.add(4);
		myFriends.add(5);
		myFriends.add(6);

		// Get spark context and add collection to paralleize method
		JavaRDD<Integer> nosRDD = sc.getJavaSparkContext().parallelize(myFriends);

		// reduceByKey is a tranformation which returns a PairRdd by combining value of
		// keys with the same key
		int total = nosRDD.reduce((a, b) -> a + b);
		System.out.println("Total of all nos : " + total);
	}

}