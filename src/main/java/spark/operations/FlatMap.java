package main.java.spark.operations;

import main.java.spark.Common;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Iterator;

public class FlatMap {
    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext sc = Common.getSc();
        JavaRDD<String> javaRDD = Common.textFile(sc);

        JavaRDD<String> mapedRdd = javaRDD.flatMap(FlatMap::oper);

        mapedRdd.foreach(Common::println);

        sc.close();
    }

    private static Iterator<String> oper(String line) {
        //2100001,北京,汤贺静,2007.4.5,女
        String[] cs = line.split(",");
        return Arrays.asList(cs[2], "入职时间：" + cs[3], "性别：" + cs[4]).iterator();
    }

}
