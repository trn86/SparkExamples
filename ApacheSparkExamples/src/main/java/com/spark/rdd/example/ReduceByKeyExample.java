package com.spark.rdd.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.my.spark.context.SparkContext;

import scala.Tuple2;

/**
 * Program to get name and count of all my friends with the same name
 *
 */
public class ReduceByKeyExample {

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

		// mapToPair is a transformation which converts each object of rdd to pair of
		// Tuple with 2 values
		JavaPairRDD<String, Integer> friendsPairRDD = frndsRDD.mapToPair(x -> new Tuple2<String, Integer>(x, 1));

		// reduceByKey is a tranformation which returns a PairRdd by combining value of
		// keys with the same key
		friendsPairRDD = friendsPairRDD.reduceByKey((a, b) -> (a + b));
		// Collect is a action which returns a java collection of list type
		List<Tuple2<String, Integer>> opFrnds = friendsPairRDD.collect();
		System.out.println("My Friends :");
		opFrnds.forEach(x -> System.out.println("Friends with same name " + x._1 + " : " + "Total nos : " + x._2));
	}

}