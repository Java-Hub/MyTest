package main.java.spark.structuredstreaming;

import main.java.spark.structuredstreaming.jdbc.JdbcOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author caik
 * @since 2021/2/24
 */
public class TestStreaming {

	public static void main(String[] args) throws StreamingQueryException {

		SparkSession spark = SparkSession.builder().master("local").appName("TestStreaming").getOrCreate();

		StructType schema = new StructType(new StructField[]{
				new StructField("name_", DataTypes.StringType, true, Metadata.empty()),
				new StructField("age_", DataTypes.createDecimalType(), true, Metadata.empty()),
				new StructField("timestamp_", DataTypes.TimestampType,true, Metadata.empty())
		});

		Dataset<Row> df = spark.readStream()
				.format("main.java.spark.structuredstreaming.jdbc.JdbcDataSourceProvider")
				.option(JdbcOptions.DRIVER, "oracle.jdbc.driver.OracleDriver")
				.option(JdbcOptions.URL, "jdbc:oracle:thin:@172.21.11.12:1521/orcl")
				.option(JdbcOptions.TABLE, "test")
				.option(JdbcOptions.USER, "root")
				.option(JdbcOptions.PWD, "root")
				.option(JdbcOptions.TIMESTAMP, "timestamp_")
				.schema(schema)
				.load();

		df.writeStream()
				.format("console")
				.option("numRows", 100000)
				.outputMode(OutputMode.Update())
				.start()
				.awaitTermination();
	}


}
