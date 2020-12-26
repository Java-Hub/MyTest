package main.java.spark.operations;

import main.java.spark.Common;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Subtract {
    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext sc = Common.getSc();
        JavaRDD<String> javaRDD = Common.textFile(sc);

        System.out.println(javaRDD.count());

        JavaRDD<String> another = javaRDD.filter(r -> r.split(",")[1].equals("武汉"));

        System.out.println(another.count());

        JavaRDD<String> subtract = javaRDD.subtract(another);

        System.out.println(subtract.count());

        subtract.foreach(Common::println);

        sc.close();
    }

}
