package main.java.spark.structuredstreaming.jdbccontinuous;

import main.java.spark.structuredstreaming.jdbc.JdbcDataSource;
import main.java.spark.structuredstreaming.jdbc.JdbcOptions;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.StreamSourceProvider;
import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.immutable.Map;

import java.util.Optional;

/**
 * @author caik
 * @since 2021/3/11
 */
public class JdbcPrivoder implements DataSourceRegister, StreamSourceProvider, ContinuousReadSupport {
	@Override
	public String shortName() {
		return "jdbc";
	}

	@Override
	public Tuple2<String, StructType> sourceSchema(SQLContext sqlContext, Option<StructType> schema, String providerName, Map<String, String> parameters) {
		return new Tuple2<>(shortName(), schema.get());
	}

	@Override
	public Source createSource(SQLContext sqlContext, String metadataPath, Option<StructType> schema, String providerName, Map<String, String> parameters) {
		SparkSession sparkSession = sqlContext.sparkSession();
		StructType structType = schema.get();
		main.java.spark.structuredstreaming.jdbc.JdbcOptions jdbcOptions = new JdbcOptions(parameters);
		return new JdbcDataSource(sparkSession, jdbcOptions, structType);
	}

	@Override
	public ContinuousReader createContinuousReader(Optional<StructType> schema, String checkpointLocation, DataSourceOptions options) {
		String timestamp = options.get("timestamp").get();
		java.util.Map<String, String> javaMap = options.asMap();
		scala.collection.mutable.Map<String, String> mutableMap = JavaConversions.mapAsScalaMap(javaMap);
		Map<String, String> immutableMap = ScalaUtils.convertMap(mutableMap);
		JDBCOptions jdbcOptions = new JDBCOptions(immutableMap);
		SparkSession sparkSession = SparkSession.active();
		return new JdbcContinuousReader(sparkSession, jdbcOptions, timestamp);
	}
}
