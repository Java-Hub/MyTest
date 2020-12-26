package main.java.spark.operations;

import main.java.spark.Common;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class Filter {
    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext sc = Common.getSc();
        JavaRDD<String> javaRDD = Common.textFile(sc);

        System.out.println(javaRDD.count());

        JavaRDD<String> filter = javaRDD.filter(Filter::oper);

        filter.foreach(Common::println);

        System.out.println(filter.count());

        sc.close();
    }

    private static boolean oper(String value) {
        return value.split(",")[1].equals("武汉");
    }
}
