package main.java.spark.structuredstreaming;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

/**
 * @author caik
 * @since 2021/2/23
 */
public class TestKafkaOutput {

	public static void main(String[] args) throws StreamingQueryException {
		SparkSession spark = SparkSession.builder().master("local").appName("TestStructuredStreaming").getOrCreate();

		spark.streams().addListener(new StreamingListener());

		Dataset<Row> df = spark.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", "petabase-1.esen.com:6667")
				.option("subscribe", "ck")
				.option("failOnDataLoss", false)
				.option("startingOffsets", "{\"ck\":{\"0\":-1}}")
				.load();

		Dataset<Row> dataset = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

		KeyValueGroupedDataset<String, Row> keyValueGroupedDataset = dataset.groupByKey((MapFunction<Row, String>) r -> r.getString(0), Encoders.STRING());

		KeyValueGroupedDataset<String, Integer> mapValues = keyValueGroupedDataset.mapValues((MapFunction<Row, Integer>) r -> Integer.parseInt(r.getString(1)), Encoders.INT());

		Dataset<Tuple2<String, Integer>> tuple2Dataset = mapValues.reduceGroups((ReduceFunction<Integer>) Integer::sum);

		StructType schema = new StructType(new StructField[]{
				new StructField("key", DataTypes.StringType, true, Metadata.empty()),
				new StructField("value", DataTypes.IntegerType, true, Metadata.empty()) });

		Dataset<Row> map = tuple2Dataset.map((MapFunction<Tuple2<String, Integer>, Row>) v -> RowFactory.create(v._1,v._2), RowEncoder.apply(schema));

		dataset = map.orderBy("value");

		StreamingQuery query = dataset.writeStream()
				.format("console")
				.option("numRows", 100000)
				.outputMode(OutputMode.Complete())
				.queryName("排序")
				.start();

		System.out.println("启动成功");

		query.awaitTermination();

	}

}
