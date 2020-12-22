package spark;

import java.util.Arrays;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

public class TestDelete {

	public static void main(String[] args) throws InterruptedException {
		SparkSession sparkSession = SparkSession.builder().master("local").appName("TestDelete").getOrCreate();
		SparkContext sparkContext = sparkSession.sparkContext();
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);
		JavaRDD<String> javaRDD = javaSparkContext.parallelize(Arrays.asList("张三", "李四"));
		JavaRDD<Row> map = javaRDD.map(row->RowFactory.create(row.split("")));
		MyMethod mm = new MyMethod();
		map.foreach(mm::print);
		javaSparkContext.close();

	}

}

class MyMethod{
	void print(Object obj) {
		System.out.println(String.valueOf(obj));
	}
}
