package main.java.spark.structuredstreaming.jdbc;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCPartition;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function0;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.immutable.HashMap;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * JDBC输入源
 * Structured Streaming原生不支持JDBC输入源
 * @author caik
 * @since 2021/3/3
 */
public class JdbcDataSource implements Source {

	private final SparkSession sc;

	private final JdbcOptions jdbc;

	private final StructType schema;

	private String[] columns;

	private Function0<Connection> getConnection;

	private Connection connection;

	public static final String NAME = "jdbc";

	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

	public JdbcDataSource(SparkSession sc, JdbcOptions jdbcOptions, StructType schema) {
		this.sc = sc;
		this.jdbc = jdbcOptions;
		this.schema = schema;
		this.init();
	}

	private void init() {
		JDBCOptions options = jdbc.asJDBCOptions();
		getConnection = JdbcUtils.createConnectionFactory(options);
		connection = getConnection.apply();
		this.columns = new String[this.schema.length()];
		Iterator<StructField> iterator = this.schema.iterator();
		int i = 0;
		while (iterator.hasNext()) {
			this.columns[i++] = iterator.next().name();
		}
	}

	@Override
	public StructType schema() {
		return schema;
	}

	@Override
	public Option<Offset> getOffset() {
		String offsetField = jdbc.getTimestamp();
		String sql = "select max(" + offsetField + ") from " + jdbc.getTable() + " T ";
		try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
			Timestamp timestamp;
			if (rs.next() && (timestamp = rs.getTimestamp(1)) != null) {
				LocalDateTime localDateTime = timestamp.toLocalDateTime();
				String dateTime = FORMATTER.format(localDateTime);
				return Option.apply(new JdbcStreamSourceOffset(dateTime));
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return Option.empty();
	}

	@Override
	public Dataset<Row> getBatch(Option<Offset> start, Offset end) {
		String startTime = start.isEmpty() ? "" : ((JdbcStreamSourceOffset) start.get()).getDateTime();
		String endTime = ((JdbcStreamSourceOffset) end).getDateTime();
		String offsetField = jdbc.getTimestamp();
		Filter[] filters = new Filter[0];
		JDBCOptions options = new JDBCOptions(jdbc.getUrl(), jdbc.getTable(), new HashMap<>());

		String where = offsetField + ">" + funcToDateTime(startTime) + " and " + offsetField + "<=" + funcToDateTime(endTime);

		JDBCPartition jdbcPartition = new JDBCPartition(where, 0);
		Partition[] partitions = { jdbcPartition };

		JDBCRDD jdbcrdd = new JDBCRDD(sc.sparkContext(), getConnection, schema, columns, filters, partitions, jdbc.getUrl(), options);
		return sc.internalCreateDataFrame(jdbcrdd.setName(NAME), schema, true);
	}

	@Override
	public void commit(Offset end) {

	}

	@Override
	public void stop() {
		try {
			connection.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private String funcToDateTime(String value) {
		String dt = StringUtils.isBlank(value) ? "0001-01-01 00:00:00:000" : value;
		return "to_timestamp('" + dt + "','yyyy-MM-dd HH24:mi:ss:ff')";
	}
}
