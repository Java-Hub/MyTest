package main.java.spark.structuredstreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * @author caik
 * @since 2021/2/23
 */
public class TestKafkaOutput {

	public static void main(String[] args) throws StreamingQueryException {
		SparkSession spark = SparkSession.builder().master("local").appName("TestStructuredStreaming").getOrCreate();

		Dataset<Row> df = spark.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", "petabase-1.esen.com:6667")
				.option("subscribe", "ck")
				.option("failOnDataLoss", false)
				.option("startingOffsets", "{\"ck\":{\"0\":-1}}")
				.load();

		Dataset<Row> rowDataset = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

//		KeyValueGroupedDataset<String, Row> keyValueGroupedDataset = rowDataset.groupByKey((MapFunction<Row, String>) r -> r.getString(0), Encoders.STRING());
//
//		KeyValueGroupedDataset<String, Integer> mapValues = keyValueGroupedDataset.mapValues((MapFunction<Row, Integer>) r -> Integer.parseInt(r.getString(1)), Encoders.INT());
//
//		Dataset<Tuple2<String, Integer>> tuple2Dataset = mapValues.reduceGroups((ReduceFunction<Integer>) Integer::sum);
//
//		StructType schema = new StructType(new StructField[]{
//				new StructField("key", DataTypes.StringType, true, Metadata.empty()),
//				new StructField("value", DataTypes.IntegerType, true, Metadata.empty()) });
//
//		Dataset<Row> map = tuple2Dataset.map((MapFunction<Tuple2<String, Integer>, Row>) v -> RowFactory.create(v._1,v._2), RowEncoder.apply(schema));
//
//		Dataset<Row> dataset = map.orderBy("value");

		StreamingQuery query = rowDataset.writeStream()
				.format("console")
				.option("numRows", 100000)
				.outputMode(OutputMode.Append())
				.start();

		System.out.println("启动成功");

		query.awaitTermination();

	}

}
