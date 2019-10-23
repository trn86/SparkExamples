package com.spark.rdd.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.my.spark.context.SparkContext;

/**
 * Program to filter records from rdd starting with A or K
 *
 */
public class FilterExample {

	public static void main(String[] args) {
		SparkContext sc = SparkContext.getSparkContext();
		List<String> myFriends = new ArrayList<String>();
		myFriends.add("Amit");
		myFriends.add("Kumar");
		myFriends.add("Samit");
		myFriends.add("Amit");
		myFriends.add("Kumar");
		myFriends.add("Samit");

		// Get spark context and add collection to paralleize method
		JavaRDD<String> frndsRDD = sc.getJavaSparkContext().parallelize(myFriends);

		// filter is a transformation which filters records as per the filter condition
		// specified
		frndsRDD = frndsRDD.filter(x -> x.startsWith("K") || x.startsWith("A"));

		// Collect is a action which returns a java collection of list type
		List<String> opFrnds = frndsRDD.collect();
		System.out.println("My Friends :");
		opFrnds.forEach(x -> System.out.println(x));
	}

}