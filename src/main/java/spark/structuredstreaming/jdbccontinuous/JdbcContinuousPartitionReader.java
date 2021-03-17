package main.java.spark.structuredstreaming.jdbccontinuous;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousInputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;
import org.apache.spark.sql.types.StructType;
import scala.Option;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author caik
 * @since 2021/3/11
 */
public class JdbcContinuousPartitionReader implements ContinuousInputPartitionReader<InternalRow>, Serializable {

	private static final long serialVersionUID = -5437762573845117127L;

	private final SparkSession sparkSession;

	private final JDBCOptions options;

	private final ArrayBlockingQueue<Row> queue;

	private volatile JdbcPartitionOffset currentOffset = JdbcPartitionOffset.apply(Option.empty());

	private transient Thread jdbcReadThread;

	private final String timestamp;

	private int timestampIndex;

	private Timestamp commitOffset = new Timestamp(0);

	private Row current;

	private StructType schema;

	private Connection connection;

	public JdbcContinuousPartitionReader(SparkSession sparkSession, JDBCOptions options, String timestamp) {
		this.sparkSession = sparkSession;
		this.options = options;
		this.queue = new ArrayBlockingQueue<>(options.batchSize());
		this.timestamp = timestamp;
	}

	private void initConnection() {
		if (connection == null) {
			try {
				Class.forName(options.driverClass());
				connection = DriverManager.getConnection(options.url(), options.asConnectionProperties());
			} catch (ClassNotFoundException | SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public void setStartOffset(JdbcPartitionOffset offset) {
		this.currentOffset = offset;
	}

	public Timestamp getCommitOffset() {
		return commitOffset;
	}

	public void start() {
		jdbcReadThread = new Thread(() -> {
			initConnection();
			StructType schema = readSchema();
			while (true) {
				String where = timestamp + "> to_timestamp('" + currentOffset.getAsString() + "', 'yyyy-MM-dd HH24:mi:ss:ff')";
				String sql = "select * from " + options.tableOrQuery() + " T where " + where;
				try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
					while (rs.next()) {
						Object[] data = FieldUtils.getWithType(rs, schema);
						GenericRowWithSchema row = new GenericRowWithSchema(data, schema);
						queue.put(row);
						currentOffset = new JdbcPartitionOffset(rs.getTimestamp(timestamp));
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				if(Thread.currentThread().isInterrupted()){
					return;
				}
			}
		});
		jdbcReadThread.setDaemon(true);
		jdbcReadThread.start();
	}

	public StructType readSchema() {
		if (schema != null) {
			return schema;
		}
		SparkSession sparkSession = SparkSession.getActiveSession().get();
		Dataset<Row> dataset = sparkSession.read().jdbc(options.url(), options.tableOrQuery(), options.asConnectionProperties());
		schema = dataset.schema();
		this.timestampIndex = schema.fieldIndex(timestamp);
		return schema;
	}

	@Override
	public boolean next() throws IOException {
		try {
			current = queue.take();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		return true;
	}

	@Override
	public InternalRow get() {
		long aLong = current.getLong(timestampIndex);
		commitOffset = new Timestamp(aLong / 1000);
		return InternalRow.apply(current.toSeq());
	}

	@Override
	public void close() throws IOException {
		if (jdbcReadThread != null) {
			jdbcReadThread.interrupt();
		}
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public PartitionOffset getOffset() {
		return new JdbcPartitionOffset(commitOffset);
	}
}
