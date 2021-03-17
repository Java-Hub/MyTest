package main.java.spark.structuredstreaming.jdbccontinuous;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

/**
 * @author caik
 * @since 2021/3/15
 */
class JdbcPartition implements InputPartition<InternalRow> {

	private static final long serialVersionUID = 443541333388135639L;

	private final JdbcContinuousPartitionReader reader;

	public JdbcPartition(SparkSession sparkSession, JDBCOptions options, String timestamp) {
		reader = new JdbcContinuousPartitionReader(sparkSession, options, timestamp);
	}

	@Override
	public InputPartitionReader<InternalRow> createPartitionReader() {
		reader.start();
		return reader;
	}

	public StructType readSchema() {
		return reader.readSchema();
	}

	public void setStartOffset(JdbcPartitionOffset offset) {
		reader.setStartOffset(offset);
	}

	public void close() throws IOException {
		reader.close();
	}

}
