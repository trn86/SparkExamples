package com.spark.rdd.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.my.spark.context.SparkContext;

import scala.Tuple2;

/**
 * Example of subtract,return records from left rdd,with records not present in
 * right side rdd
 *
 */
public class SubtractExample {

	public static void main(String[] args) {
		SparkContext sc = SparkContext.getSparkContext();
		List<String> myFriends = new ArrayList<String>();
		myFriends.add("Amit");
		myFriends.add("Kumar");
		myFriends.add("Samit");

		List<String> strangerFriends = new ArrayList<String>();
		strangerFriends.add("Bhaves");
		strangerFriends.add("Amit");
		strangerFriends.add("Saxena");

		// Get spark context and add collection to paralleize method
		JavaRDD<String> myfrndsRDD = sc.getJavaSparkContext().parallelize(myFriends);
		JavaRDD<String> strangerFriendsRDD = sc.getJavaSparkContext().parallelize(strangerFriends);

		// mapToPair is a transformation which converts each object of rdd to pair of
		// Tuple with 2 values
		JavaPairRDD<String, Integer> myfriendsPairRDD = myfrndsRDD.mapToPair(x -> new Tuple2<String, Integer>(x, 1));
		JavaPairRDD<String, Integer> strangerFriendsPairRDD = strangerFriendsRDD
				.mapToPair(x -> new Tuple2<String, Integer>(x, 1));

		// subtract is a transformation ,return records from left rdd,with records not
		// present in
		// right side rdd
		JavaPairRDD<String, Integer> commonFriendsRDD = myfriendsPairRDD.subtract(strangerFriendsPairRDD);

		// Collect is a action which returns a java collection of list type
		List<Tuple2<String, Integer>> opFrnds = commonFriendsRDD.collect();
		System.out.println("Non Common My Friends :");
		opFrnds.forEach(x -> System.out.println(x._1 + " : " + x._2));
	}

}
