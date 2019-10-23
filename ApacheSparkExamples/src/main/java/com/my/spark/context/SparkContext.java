package com.my.spark.context;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Creates a spark context required to create rdd.Befor spark 1.6.3 we used this.
 *
 */
public class SparkContext {

	private static SparkContext sparkContext;
	private static JavaSparkContext cntx;

	private SparkContext() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("RDD Task");
		cntx = new JavaSparkContext(conf);
	}

	public static SparkContext getSparkContext() {
		if (sparkContext == null) {
			sparkContext = new SparkContext();
		}
		return sparkContext;
	}

	public static JavaSparkContext getJavaSparkContext() {
		return cntx;
	}

}