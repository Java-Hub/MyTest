package main.java.spark.structuredstreaming.jdbccontinuous;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * @author caik
 * @since 2021/3/12
 */
public class JdbcContinuousReader implements ContinuousReader {

	private JdbcPartitionOffset offset;

	private final JdbcPartition partition;

	public JdbcContinuousReader(SparkSession sparkSession, JDBCOptions options, String timestamp) {
		partition = new JdbcPartition(sparkSession, options, timestamp);
	}

	@Override
	public Offset mergeOffsets(PartitionOffset[] offsets) {
		offset = Arrays.stream(offsets).reduce(offset, JdbcPartitionOffset::merge, JdbcPartitionOffset::merge);
		return offset;
	}

	@Override
	public Offset deserializeOffset(String json) {
		return JdbcPartitionOffset.read(json);
	}

	@Override
	public void setStartOffset(Optional<Offset> start) {
		this.offset = JdbcPartitionOffset.apply(start);
		partition.setStartOffset(offset);
	}

	@Override
	public Offset getStartOffset() {
		return this.offset;
	}

	@Override
	public void commit(Offset end) {

	}

	@Override
	public void stop() {
		try {
			partition.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public StructType readSchema() {
		return partition.readSchema();
	}

	@Override
	public List<InputPartition<InternalRow>> planInputPartitions() {
		return Collections.singletonList(partition);
	}

}
