package main.java.spark.structuredstreaming.jdbccontinuous;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
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

		SparkSession spark = SparkSession.builder().master("local[*]").appName("TestStreaming").getOrCreate();

		StructType schema = new StructType(
				new StructField[] { new StructField("NAME_", DataTypes.StringType, true, Metadata.empty()), new StructField("AGE_", DataTypes.createDecimalType(), true, Metadata.empty()),
						new StructField("TIMESTAMP_", DataTypes.TimestampType, true, Metadata.empty()) });

		Dataset<Row> df = spark.readStream()
				.format("main.java.spark.structuredstreaming.jdbccontinuous.JdbcPrivoder")
				.option(JDBCOptions.JDBC_DRIVER_CLASS(), "oracle.jdbc.driver.OracleDriver")
				.option(JDBCOptions.JDBC_URL(), "jdbc:oracle:thin:@172.21.11.12:1521/orcl")
				.option(JDBCOptions.JDBC_TABLE_NAME(), "test")
				.option("user", "root")
				.option("password", "root")
				.option("timestamp", "TIMESTAMP_")
				.schema(schema)
				.load();

				df.writeStream()
						.format("console")
						.option("numRows", 100)
						.option("truncate", 100)
						.option("checkpointLocation", "file:///E:\\testcheckpoint")
						.outputMode(OutputMode.Update())
						.trigger(Trigger.Continuous(0))
						.start()
						.awaitTermination();
	}

}
