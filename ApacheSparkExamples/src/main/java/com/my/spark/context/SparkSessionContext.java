package com.my.spark.context;

import org.apache.spark.sql.SparkSession;

/**
 * Creates a spark session which can be used to get spark context to create
 * rdd,sql context for spark sql,hive context for working with hike and streaming context.
 * From spark 2.0 Spark session provides all the context
 *
 */
public class SparkSessionContext {

	private static SparkSessionContext sparkSessionContext;
	private static SparkSession session;

	private SparkSessionContext() {
		session = SparkSession.builder().appName("DF Task").master("local[*]").getOrCreate();
	}

	public static SparkSessionContext getSessionSparkContext() {
		if (sparkSessionContext == null) {
			sparkSessionContext = new SparkSessionContext();
		}
		return sparkSessionContext;
	}

	public static SparkSession getSession() {
		return session;
	}

}