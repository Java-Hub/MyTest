package main.java.spark.structuredstreaming.jdbccontinuous;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;
import scala.Option;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

/**
 * JDBC偏移量，只支持时间戳偏移量
 * @author caik
 * @since 2021/3/12
 */
public class JdbcPartitionOffset extends Offset implements PartitionOffset {

	private static final long serialVersionUID = 3710655962138822410L;

	private Long timestamp;

	private static final Gson GSON = new GsonBuilder().create();

	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSSSSS");

	public JdbcPartitionOffset(Timestamp timestamp) {
		this.timestamp = timestamp.getTime();
	}

	public Timestamp getTimestamp() {
		return new Timestamp(timestamp);
	}

	public long milliseconds() {
		return this.timestamp;
	}

	public String getAsString() {
		return getTimestamp().toLocalDateTime().format(FORMATTER);
	}

	public JdbcPartitionOffset merge(PartitionOffset other) {
		if (other instanceof JdbcPartitionOffset) {
			JdbcPartitionOffset o = (JdbcPartitionOffset) other;
			return merge(o.timestamp);
		}
		return this;
	}

	public JdbcPartitionOffset merge(long dateTime) {
		this.timestamp = Math.max(timestamp, dateTime);
		return this;
	}

	public static JdbcPartitionOffset apply(Offset offset) {
		return offset == null ? new JdbcPartitionOffset(new Timestamp(0)) : read(offset.json());
	}

	public static JdbcPartitionOffset apply(Option<Offset> option) {
		Offset offset = option.isEmpty() ? null : option.get();
		return apply(offset);
	}

	public static JdbcPartitionOffset apply(Optional<Offset> optional) {
		return apply(optional.orElse(null));
	}

	public static JdbcPartitionOffset read(String json) {
		if (StringUtils.isBlank(json) || json.equalsIgnoreCase("{}")) {
			return new JdbcPartitionOffset(new Timestamp(0));
		} else {
			return GSON.fromJson(json, JdbcPartitionOffset.class);
		}
	}

	@Override
	public String json() {
		return GSON.toJson(this);
	}
}
