package main.java.spark.operations.bykey;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import main.java.spark.Common;
import scala.Tuple2;

public class CombineByKey {
	public static void main(String[] args) throws InterruptedException {
		JavaSparkContext sc = Common.getSc();
		JavaRDD<String> javaRDD = Common.textFile(sc);

		JavaPairRDD<String, Integer> javaPairRDD = javaRDD.mapToPair(str -> {
			String[] split = str.split(",");
			String[] year = split[3].split("\\.");
			return new Tuple2<>(split[1], Integer.parseInt(year[0]));
		});
		javaPairRDD.foreach(x -> System.out.println(x._1 + ":" + x._2));

		JavaPairRDD<String, String> pairRDD = javaRDD.keyBy(str -> str.split(",")[1]);
		JavaPairRDD<String, Integer> pairRDD1 = pairRDD.mapValues(str -> Integer.parseInt(str.split(",")[3].split("\\.")[0]));
		pairRDD1.foreach(row -> System.out.println(row._1 + ":" + row._2));

		System.out.println(javaPairRDD.count());
		System.out.println(pairRDD1.count());

		JavaPairRDD<String, Integer> subtract = javaPairRDD.subtract(pairRDD1);
		subtract.foreach(row -> System.out.println(row._1 + ":" + row._2));
		System.out.println(subtract.count());

		JavaPairRDD<String, Tuple2<Integer, Integer>> javaPairRDD1 = javaPairRDD.combineByKey(
														x -> new Tuple2<Integer, Integer>(1, x), 
														(x, y) -> new Tuple2<>(x._1 + 1, x._2 + y),
														(x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2));

		JavaPairRDD<String, Integer> javaPairRDD2 = javaPairRDD1.mapValues(x -> x._2 / x._1);
		javaPairRDD2.foreach(x -> System.out.println(x._1 + ":" + x._2));

		sc.close();
	}

}
