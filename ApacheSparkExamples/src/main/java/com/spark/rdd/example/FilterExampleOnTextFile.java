package com.spark.rdd.example;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.my.spark.context.SparkContext;

import scala.Tuple2;

/**
 * Program to filter records from rdd starting with A or K
 *
 */
public class FilterExampleOnTextFile {

	public static void main(String[] args) {
		SparkContext sc = SparkContext.getSparkContext();

		String path = "src/main/resources/story.txt";
		// Get spark context and add collection to paralleize method
		JavaRDD<String> storyRDD = sc.getJavaSparkContext().textFile(path);

		// filter is a transformation which filters records as per the filter condition
		// specified
		storyRDD = storyRDD.filter(x -> x != null);

		// Create pair rdd
		JavaPairRDD<String, Integer> wordsPairRDD = storyRDD.mapToPair(x -> new Tuple2<String, Integer>(x, 1));

		// Reduce/combine
		wordsPairRDD = wordsPairRDD.reduceByKey((a, b) -> a + b);
		// Collect is a action which returns a java collection of list type
		List<Tuple2<String, Integer>> wordsLst = wordsPairRDD.collect();
		System.out.println("All Words :");
		wordsLst.forEach(x -> System.out.println(x._1 + " : " + " Count : " + x._2));
	}

}