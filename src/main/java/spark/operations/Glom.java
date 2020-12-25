package main.java.spark.operations;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import main.java.spark.Common;

public class Glom {
    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext sc = Common.getSc();
        JavaRDD<String> javaRDD = Common.textFile(sc);

        JavaRDD<List<String>> glom = javaRDD.glom();

        JavaRDD<String> map = glom.map(Glom::oper);

        map.foreach(Common::println);

        sc.close();
    }

    private static String oper(List<String> lines) {
        Stream<String> stream = lines.stream();
        Optional<String> min = stream.min(Comparator.comparingInt(o -> Integer.parseInt(o.split(",")[0])));
        return min.get();
    }
}
