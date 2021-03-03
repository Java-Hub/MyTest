package main.java.spark.structuredstreaming.jdbc;

import org.apache.spark.sql.execution.streaming.Offset;

/**
 * JDBC偏移量，只支持时间戳偏移量
 * @author caik
 * @since 2021/3/3
 */
public class JdbcStreamSourceOffset extends Offset {

	private String dateTime;

	public JdbcStreamSourceOffset(String dateTime) {
		this.dateTime = dateTime;
	}

	public String getDateTime() {
		return dateTime;
	}

	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}

	@Override
	public String json() {
		return "{\"dateTime\"" + dateTime + "}";
	}
}
