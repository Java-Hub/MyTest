package main.java.spark.operations;

import main.java.spark.Common;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MapPartitions {
    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext sc = Common.getSc();
        JavaRDD<String> javaRDD = Common.textFile(sc);

        JavaRDD<String> mapedRdd = javaRDD.mapPartitions(MapPartitions::oper);

        mapedRdd.foreach(Common::println);

        sc.close();
    }

    private static Iterator<String> oper(Iterator<String> lines) {
        List<String> list = new ArrayList<>();
        lines.forEachRemaining(r-> {
            String[] v = r.split(",");
            list.add(v[2]+"-"+v[4]);
        });
        return list.iterator();
    }
}
