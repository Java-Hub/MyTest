package main.java.spark.structuredstreaming.jdbc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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

	private Connection connection;

	private String[] columns;

	public static final String NAME = "jdbc";

	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

	public JdbcDataSource(SparkSession sc, JdbcOptions jdbcOptions, StructType schema) {
		this.sc = sc;
		this.jdbc = jdbcOptions;
		this.schema = schema;
		this.init();
	}

	private void init() {
		String url = jdbc.getUrl();
		String user = jdbc.getUser();
		String pwd = jdbc.getPwd();
		try {
			Class.forName(jdbc.getDriver());
			this.connection = DriverManager.getConnection(url, user, pwd);
		} catch (SQLException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
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
			if (rs.next()) {
				Timestamp timestamp = rs.getTimestamp(1);
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
		String startTime = start.isEmpty() ? "''" : ((JdbcStreamSourceOffset) start.get()).getDateTime();
		String endTime = ((JdbcStreamSourceOffset) end).getDateTime();
		String offsetField = jdbc.getTimestamp();
		Filter[] filters = new Filter[] { new LessThan(offsetField, funcToDateTime(startTime)), new GreaterThan(offsetField, funcToDateTime(endTime)) };
		JDBCOptions options = new JDBCOptions(jdbc.getUrl(), jdbc.getTable(), new HashMap<>());
		JDBCRDD jdbcrdd = new JDBCRDD(sc.sparkContext(), ()->connection, schema, columns, filters, null, jdbc.getUrl(), options);
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
		return "to_timestamp('" + value + "','yyyy-MM-dd HH24:mi:ss:ff')";
	}
}
