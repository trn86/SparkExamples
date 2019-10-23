package com.spark.rdd.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.my.spark.context.SparkContext;

import scala.Tuple2;

/**
 * Groups records with same key in one partition.Glom contains element in the partition
 *
 */
public class GroupByExample {

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
		
		// mapToPair is a transformation which converts each object of rdd to pair of Tuple with 2 values 
		JavaPairRDD<String, Integer> friendsPairRDD = frndsRDD.mapToPair(x -> new Tuple2<String, Integer>(x, 1));
		
		// groupBy is a transformation which returns a PairRdd by combining value of keys with the same key
		JavaPairRDD<Object, Iterable<Tuple2<String, Integer>>> friendsPairRDDOp = friendsPairRDD.groupBy(x -> x._1);
		System.out.println("My Friends in each partition:");

		// glom is a pransformation which contains element in each partition
		friendsPairRDDOp.glom().foreach(x -> System.out.println(x));
	}

}