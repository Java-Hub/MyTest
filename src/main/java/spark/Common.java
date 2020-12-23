package main.java.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class Common {

    private static final String TEXT_FILE_PATH = "e:\\spark\\info.txt";

    public static JavaSparkContext getSc() {
        SparkSession sparkSession = SparkSession.builder().appName(Thread.currentThread().getName()).master("local").getOrCreate();
        return new JavaSparkContext(sparkSession.sparkContext());
    }

    public static JavaRDD<String> textFile(JavaSparkContext sparkContext){
        return sparkContext.textFile(TEXT_FILE_PATH);
    }

    public static void println(Object obj) {
        System.out.println(obj);
    }
}
