package spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class TestJDBC {

	static String table = "DATA_1000";

	@Test
	public void spark() {
		SparkSession sparkSession = SparkSession.builder().master("local").appName("TestSpark").getOrCreate();
		sparkSession.sparkContext().setLogLevel(Level.WARN.toString());
		SQLContext sqlContext = sparkSession.sqlContext();

		long start = System.currentTimeMillis();
		System.out.println("开始：" + start);

		Dataset<Row> dataset = sqlContext.read().format("jdbc").option("url", "jdbc:oracle:thin:@localhost:1521/orcl").option("dbtable", table).option("user", "root").option("password", "root").load();

		dataset.collect();
		long end = System.currentTimeMillis();
		System.out.println("耗时(毫秒)：" + (end - start));

	}

	@Test
	public void jdbc() throws Exception {
		SparkSession sparkSession = SparkSession.builder().master("local").appName("TestSpark").getOrCreate();
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
		javaSparkContext.setLogLevel(Level.WARN.toString());
		SQLContext sqlContext = sparkSession.sqlContext();

		Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/orcl", "root", "root");
		Statement stmt = conn.createStatement();

		long start = System.currentTimeMillis();
		System.out.println("开始：" + start);

		//设置字段结构
		StructType type = new StructType();
		type = type.add("ID_", DataTypes.StringType);
		type = type.add("COL1", DataTypes.createDecimalType());
		type = type.add("COL2", DataTypes.StringType);
		type = type.add("COL3", DataTypes.StringType);
		type = type.add("COL4", DataTypes.StringType);
		type = type.add("COL5", DataTypes.StringType);
		type = type.add("COL6", DataTypes.StringType);
		type = type.add("COL7", DataTypes.StringType);
		type = type.add("COL8", DataTypes.StringType);
		type = type.add("COL9", DataTypes.StringType);

		JavaRDD<Row> rdd = javaSparkContext.emptyRDD();

		String sql = "SELECT ID_,COL1,COL2,COL3,COL4,COL5,COL6,COL7,COL8,COL9 FROM " + table;
		Class.forName("oracle.jdbc.driver.OracleDriver");
		try (ResultSet rs = stmt.executeQuery(sql)) {
			int colClunt = type.length();
			List<Row> list = new ArrayList<>();
			int batch = 1000;
			while (rs.next()) {
				Object[] data = new Object[colClunt];
				for (int i = 1; i <= colClunt; i++) {
					data[i - 1] = rs.getObject(i);
				}
				//将每行数据转换为Row，并存入list
				Row row = RowFactory.create(data);
				list.add(row);
				if (list.size() % batch == 0) {
					JavaRDD<Row> javaRDD = javaSparkContext.parallelize(list);
					rdd = rdd.union(javaRDD);
					list = new ArrayList<>();
				}
			}
			if (list.size() > 0) {
				JavaRDD<Row> javaRDD = javaSparkContext.parallelize(list);
				rdd = rdd.union(javaRDD);
			}
		}

		Dataset<Row> dataset = sqlContext.createDataFrame(rdd.rdd(), type);

		dataset.collect();

		long end = System.currentTimeMillis();
		System.out.println("开始：" + end);
		System.out.println("耗时(毫秒)：" + (end - start));
		
		stmt.close();
		conn.close();

	}

}
