package main.java.spark.operations;

import main.java.spark.Common;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class GroupBy {
    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext sc = Common.getSc();
        JavaRDD<String> javaRDD = Common.textFile(sc);

        JavaPairRDD<String, Iterable<String>> rdd = javaRDD.groupBy(GroupBy::oper);

        rdd.foreach(tup -> {
            System.out.println(tup._1 + ":");
            tup._2.forEach(row -> System.out.println("\t" + row));
        });

        rdd.foreachPartition(iterator->{
            long count = Stream.of(iterator).count();
        });

        rdd.foreachPartition(iterator->{
            iterator.forEachRemaining(r-> System.out.println(r._1));
        });

        rdd.foreachPartition(iterator->{
          iterator.forEachRemaining(r-> r._2.forEach(System.out::println));
        });

        sc.close();
    }

    /**
     * 将cs[1]作为Key
     * @param line
     * @return
     */
    private static String oper(String line) {
        String[] cs = line.split(",");
        return cs[1];
    }
}
