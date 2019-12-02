package com.spark.rdd.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.my.spark.context.SparkContext;

import scala.Tuple2;

public class CoGroup2 {
	public static void main(String[] args) {
		SparkContext sc = SparkContext.getSparkContext();

		List<SerialNums> serialNumsLst = new ArrayList<SerialNums>();
		List<ActivationCodes> asctivationCodesLst = new ArrayList<ActivationCodes>();
		List<Voucher> voucherLst = new ArrayList<Voucher>();

		SerialNums serialNums1 = new SerialNums("11", "aa");
		SerialNums serialNums2 = new SerialNums("22", "bb");
		SerialNums serialNums3 = new SerialNums("33", "cc");
		SerialNums serialNums4 = new SerialNums("44", "dd");
		serialNumsLst.add(serialNums1);
		serialNumsLst.add(serialNums2);
		serialNumsLst.add(serialNums3);
		serialNumsLst.add(serialNums4);
		ActivationCodes asctivationCodes1 = new ActivationCodes("bb", "a1");
		ActivationCodes asctivationCodes2 = new ActivationCodes("dd", "a2");
		ActivationCodes asctivationCodes3 = new ActivationCodes("ee", "a3");
		ActivationCodes asctivationCodes4 = new ActivationCodes("ff", "a4");
		asctivationCodesLst.add(asctivationCodes1);
		asctivationCodesLst.add(asctivationCodes2);
		asctivationCodesLst.add(asctivationCodes3);
		asctivationCodesLst.add(asctivationCodes4);
		Voucher voucher1 = new Voucher("zz", "v1");
		Voucher voucher2 = new Voucher("bb", "v2");
		Voucher voucher3 = new Voucher("qq", "v3");
		Voucher voucher4 = new Voucher("dd", "v4");

		voucherLst.add(voucher1);
		voucherLst.add(voucher2);
		voucherLst.add(voucher3);
		voucherLst.add(voucher4);

		// Get spark context and add collection to paralleize method
		JavaRDD<SerialNums> serialNumsRDD = sc.getJavaSparkContext().parallelize(serialNumsLst);
		JavaRDD<ActivationCodes> activationCodesRDD = sc.getJavaSparkContext().parallelize(asctivationCodesLst);
		JavaRDD<Voucher> voucherRDD = sc.getJavaSparkContext().parallelize(voucherLst);

		// Map key and pair
		JavaPairRDD<String, SerialNums> serialNumsPairRDD = serialNumsRDD
				.mapToPair(x -> new Tuple2<String, SerialNums>(x.getCaCode(), x));
		JavaPairRDD<String, ActivationCodes> activationCodesPairRDD = activationCodesRDD
				.mapToPair(x -> new Tuple2<String, ActivationCodes>(x.getKey(), x));
		JavaPairRDD<String, Voucher> vouchersPairRDD = voucherRDD
				.mapToPair(x -> new Tuple2<String, Voucher>(x.getKey(), x));

		// Co group for serialnums and vouchers
		JavaPairRDD<String, Tuple2<Iterable<SerialNums>, Iterable<Voucher>>> coGroupedSerialNumsRDD = serialNumsPairRDD
				.cogroup(vouchersPairRDD);
		coGroupedSerialNumsRDD
				.foreach(x -> System.out.println("SerialNums Co grouped " + x._1 + " : " + x._2._1 + " : " + x._2._2));
		JavaPairRDD<String, Tuple2<Iterable<SerialNums>, Iterable<Voucher>>> coGroupedSerialNumsRDDFinal = coGroupedSerialNumsRDD
				.filter(x -> x._2()._1.iterator().hasNext() && !x._2()._2.iterator().hasNext());
		coGroupedSerialNumsRDDFinal.foreach(x -> System.out
				.println("Filtered ->> SerialNums Co grouped " + x._1 + " : " + x._2._1 + " : " + x._2._2));
		
		// Co group for activationcodes and vouchers
		JavaPairRDD<String, Tuple2<Iterable<ActivationCodes>, Iterable<Voucher>>> coGroupedActivationNumsRDD = activationCodesPairRDD
				.cogroup(vouchersPairRDD);
		coGroupedActivationNumsRDD.foreach(
				x -> System.out.println("ActivationCodes Co grouped " + x._1 + " : " + x._2._1 + " : " + x._2._2));
		JavaPairRDD<String, Tuple2<Iterable<ActivationCodes>, Iterable<Voucher>>> coGroupedActivationNumsRDDFinal = coGroupedActivationNumsRDD
				.filter(x -> x._2()._1.iterator().hasNext() && !x._2()._2.iterator().hasNext());
		coGroupedActivationNumsRDDFinal.foreach(x -> System.out
				.println("Filtered ->> ActivationCodes Co grouped " + x._1 + " : " + x._2._1 + " : " + x._2._2));
	}
}
