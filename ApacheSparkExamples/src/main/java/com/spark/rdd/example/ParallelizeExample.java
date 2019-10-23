package com.spark.rdd.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.my.spark.context.SparkContext;

/**
 * Program to show hot to add java collection into spark and convert to RDD
 *
 */
public class ParallelizeExample {

	public static void main(String[] args) {
		SparkContext sc = SparkContext.getSparkContext();
		List<String> myFriends = new ArrayList<String>();
		myFriends.add("Amit");
		myFriends.add("Kumar");
		myFriends.add("Samit");
		
		// Get spark context and add collection to paralleize method
		JavaRDD<String> frndsRDD = sc.getJavaSparkContext().parallelize(myFriends);
		
		// Collect is a action which returns a java collection of list type
		List<String> opFrnds = frndsRDD.collect();
		System.out.println("My Friends :");
		opFrnds.forEach(x -> System.out.println(x));
	}

}