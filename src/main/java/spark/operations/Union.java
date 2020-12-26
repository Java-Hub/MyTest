package main.java.spark.operations;

import main.java.spark.Common;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Union {

    public static void main(String[] args) {
        JavaSparkContext sc = Common.getSc();
        JavaRDD<String> stringJavaRDD = Common.textFile(sc);
        JavaRDD<String> javaRDD = Common.textFile(sc);
        JavaRDD<String> union = javaRDD.union(stringJavaRDD);
        union.collect().stream().forEach(System.out::println);
    }

}
