package main.java.spark.operations;

import main.java.spark.Common;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Map {
    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext sc = Common.getSc();
        JavaRDD<String> javaRDD = Common.textFile(sc);

        JavaRDD<String> mapedRdd = javaRDD.map(Map::map);

        mapedRdd.foreach(Common::println);

        sc.close();
    }

    private static String map(String line) {
        String[] cs = line.split(",");
        return cs[2] + "-" + cs[4] + "-" + cs[1] + "-" + cs[3];
    }

}
