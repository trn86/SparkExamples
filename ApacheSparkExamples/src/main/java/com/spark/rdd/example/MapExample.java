package com.spark.rdd.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.my.spark.context.SparkContext;

/**
 * Example of map transformation,which adds string is My Friend
 *
 */
public class MapExample {

	public static void main(String[] args) {
		SparkContext sc = SparkContext.getSparkContext();
		List<String> myFriends = new ArrayList<String>();
		myFriends.add("Amit");
		myFriends.add("Kumar");
		myFriends.add("Samit");

		// Get spark context and add collection to paralleize method
		JavaRDD<String> frndsRDD = sc.getJavaSparkContext().parallelize(myFriends);

		// A map is a transformation,which converts each attribute in rdd ,to specified
		// type
		JavaRDD<String> friendsRDD = frndsRDD.map(x -> (x + " is My Friend"));

		// Collect is a action which returns a java collection of list type
		List<String> opFrnds = friendsRDD.collect();
		System.out.println("My Friends :");
		// Print each element from the list collection
		opFrnds.forEach(x -> System.out.println(x));
	}

}