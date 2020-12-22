package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class WriteDB {

	public static void main(String[] args) throws InterruptedException {
		SparkSession sparkSession = SparkSession.builder().master("local").appName("TestSpark").getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
		
		JavaRDD<String> javaRDD = sc.textFile("e:\\temp\\info.txt", 1);
		
		JavaRDD<Row> rdd = javaRDD.map(s -> RowFactory.create(s.split(",")));
		
		StructType structType = new StructType();
		structType = structType.add("ID_", DataTypes.StringType);
		structType = structType.add("NAME_", DataTypes.StringType);
		structType = structType.add("AGE_", DataTypes.StringType);
		SQLContext sqlContext = sparkSession.sqlContext();
		
		Dataset<Row> dataset = sqlContext.createDataFrame(rdd, structType);
		
		dataset.write().format("jdbc")
								.mode(SaveMode.Append)
								.option("url", "jdbc:oracle:thin:@localhost:1521/orcl")
								.option("dbtable", "testspark")
								.option("user", "root")
								.option("password", "root")
								.save();
	}
	

}
