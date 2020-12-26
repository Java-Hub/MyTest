package main.java.spark.operations;

import main.java.spark.Common;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Distinct {
    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext sc = Common.getSc();
        JavaRDD<String> javaRDD = Common.textFile(sc);

        System.out.println(javaRDD.count());

        JavaRDD<String> union = javaRDD.union(javaRDD);

        System.out.println(union.count());

        JavaRDD<String> distinct = union.distinct();

        System.out.println(distinct.count());

        sc.close();
    }

}
