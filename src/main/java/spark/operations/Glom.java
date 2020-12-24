package main.java.spark.operations;

import main.java.spark.Common;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

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
        List<String> list = new ArrayList<>();
        Stream<String> stream = lines.stream();
        Optional<String> min = stream.min((o1, o2) -> {
            return Integer.parseInt(o1.split(",")[0]) - Integer.parseInt(o2.split(",")[0]);
        });
        return min.get();
    }
}
