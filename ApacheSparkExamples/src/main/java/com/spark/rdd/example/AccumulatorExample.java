package com.spark.rdd.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.my.spark.context.SparkContext;

/**
 * Program to count no of null records in rdd.Accumlator is write only variable for workers
 *
 */
public class AccumulatorExample {

	public static void main(String[] args) {
		SparkContext sc = SparkContext.getSparkContext();
		JavaSparkContext jsc = sc.getJavaSparkContext();
		List<String> myFriends = new ArrayList<String>();
		myFriends.add(null);
		myFriends.add("Kumar");
		myFriends.add("Samit");
		myFriends.add(null);
		myFriends.add("Kumar");
		myFriends.add("Samit");

		// Get spark context and add collection to parallelize method
		JavaRDD<String> frndsRDD = jsc.parallelize(myFriends);

		Accumulator<Integer> accCount = jsc.intAccumulator(0, "Blank record Count");

		// filter is a transformation which filters records as per the filter condition
		// specified
		frndsRDD = frndsRDD.filter(x -> {
			if (x != null) {
				return true;
			} else {
				accCount.add(1);
				return false;
			}
		});
		// Need to do action,otherwise accumulator will not return the count 
		frndsRDD.collect();
		System.out.println("Blank records count is  : -------------> " + accCount.value());
	}

}