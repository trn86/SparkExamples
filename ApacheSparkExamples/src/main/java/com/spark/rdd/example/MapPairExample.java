package com.spark.rdd.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.my.spark.context.SparkContext;

import scala.Tuple2;

/**
 * Example of map transformation,which converts each rdd element into a key value pair called Tuple2
 *
 */
public class MapPairExample {

	public static void main(String[] args) {
		SparkContext sc = SparkContext.getSparkContext();
		List<String> myFriends = new ArrayList<String>();
		myFriends.add("Amit");
		myFriends.add("Kumar");
		myFriends.add("Samit");
		
		// Get spark context and add collection to paralleize method
		JavaRDD<String> frndsRDD = sc.getJavaSparkContext().parallelize(myFriends);
		
		// mapToPair is a transformation which converts each object of rdd to pair of Tuple with 2 values 
		JavaPairRDD<String, Integer> friendsPairRDD = frndsRDD.mapToPair(x -> new Tuple2<String, Integer>(x, 1));
		
		// Collect is a action which returns a java collection of list type
		List<Tuple2<String, Integer>> opFrnds = friendsPairRDD.collect();
		System.out.println("My Friends :");
		opFrnds.forEach(x -> System.out.println(x._1 + " : " + x._2));
	}

}