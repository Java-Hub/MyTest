package main.java.spark.structuredstreaming.jdbc;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.StreamSourceProvider;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map;

/**
 * @author caik
 * @since 2021/3/2
 */
public class JdbcDataSourceProvider implements DataSourceRegister, StreamSourceProvider {
	@Override
	public String shortName() {
		return JdbcDataSource.NAME;
	}

	@Override
	public Tuple2<String, StructType> sourceSchema(SQLContext sqlContext, Option<StructType> schema, String providerName, Map<String, String> parameters) {
		return new Tuple2<>(shortName(), schema.get());
	}

	@Override
	public Source createSource(SQLContext sqlContext, String metadataPath, Option<StructType> schema, String providerName, Map<String, String> parameters) {
		this.checkParamater(parameters);
		SparkSession sparkSession = sqlContext.sparkSession();
		StructType structType = schema.get();
		JdbcOptions jdbcOptions = new JdbcOptions(parameters);
		return new JdbcDataSource(sparkSession, jdbcOptions, structType);
	}

	private void checkParamater(Map<String, String> map) {
		if (!map.contains(JdbcOptions.DRIVER)) {
			throw new RuntimeException("未设置驱动路径：" + JdbcOptions.DRIVER);
		}
		if (!map.contains(JdbcOptions.URL)) {
			throw new RuntimeException("未设置连接地址：" + JdbcOptions.URL);
		}
		if (!map.contains(JdbcOptions.TABLE)) {
			throw new RuntimeException("未设置表名：" + JdbcOptions.TABLE);
		}
		if (!map.contains(JdbcOptions.USER)) {
			throw new RuntimeException("未设置用户名：" + JdbcOptions.USER);
		}
		if (!map.contains(JdbcOptions.PWD)) {
			throw new RuntimeException("未设置密码：" + JdbcOptions.PWD);
		}
		if (!map.contains(JdbcOptions.TIMESTAMP)) {
			throw new RuntimeException("未设置时间戳字段：" + JdbcOptions.TIMESTAMP);
		}
	}

}
