package main.java.spark.structuredstreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author caik
 * @since 2021/2/23
 */
public class TestKafkaInput {

	public static void main(String[] args) throws StreamingQueryException {
		SparkSession sparkSession = SparkSession.builder().master("local").appName("TestKafkaInput").getOrCreate();

		sparkSession.streams().addListener(new StreamingListener());

		StructType structType = new StructType(new StructField[] { new StructField("value", DataTypes.StringType, true, Metadata.empty()) });

		Dataset<Row> dataset = sparkSession.readStream()
				.format("text")
				.schema(structType)
				.option("path", "hdfs://localhost:9000/spark/")
				.load();

//		KeyValueGroupedDataset<String, Integer> values = dataset.groupByKey((MapFunction<Row, String>) Row::mkString, Encoders.STRING()).mapValues((MapFunction<Row, Integer>) row -> 1, Encoders.INT());
//
//		Dataset<Tuple2<String, Integer>> tuple2Dataset = values.reduceGroups((ReduceFunction<Integer>) Integer::sum);
//
//		StructType type = new StructType(	new StructField[] {
//				new StructField("key", DataTypes.StringType, true, Metadata.empty()),
//				new StructField("value", DataTypes.StringType, true, Metadata.empty()) });
//
//		Dataset<Row> rowDataset = tuple2Dataset.map((MapFunction<Tuple2<String, Integer>, Row>) v -> RowFactory.create(v._1, String.valueOf(v._2)), RowEncoder.apply(type));
//
//		tuple2Dataset.map((MapFunction<Tuple2<String, Integer>, Row>) v -> RowFactory.create(v._1, String.valueOf(v._2)), RowEncoder.apply(type))
//				.writeStream().format("console")
//				.outputMode(OutputMode.Complete())
//				.option("numRows", 1000)
//				.start();

		StreamingQuery query = dataset.writeStream()
				.format("kafka")
				.option("checkpointLocation", "file:///D:\\App\\checkpoint")
				.option("kafka.bootstrap.servers", "petabase-1.esen.com:6667")
				.option("topic", "ck")
				.outputMode(OutputMode.Update())
				.start();

		System.out.println("启动成功");

		query.awaitTermination();

	}

}
