package main.java.spark.structuredstreaming.jdbc;

import org.apache.spark.sql.execution.streaming.FileStreamSourceOffset;
import org.apache.spark.sql.execution.streaming.Offset;
import org.json4s.Formats;
import org.json4s.jackson.Serialization;
import scala.Option;
import scala.reflect.Manifest;
import scala.reflect.ManifestFactory;

/**
 * JDBC偏移量，只支持时间戳偏移量
 * @author caik
 * @since 2021/3/3
 */
public class JdbcStreamSourceOffset extends Offset {

	private String dateTime;

	private static final Formats FORMATS = (Formats) FileStreamSourceOffset.format();

	public JdbcStreamSourceOffset(String dateTime) {
		this.dateTime = dateTime;
	}

	public String getDateTime() {
		return dateTime;
	}

	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}

	public static String apply(Offset offset) {
		if (offset == null){
			return "0001-01-01 00:00:00:000000";
		}else {
			Manifest<JdbcStreamSourceOffset> classType = ManifestFactory.classType(JdbcStreamSourceOffset.class);
			return Serialization.read(offset.json(), FORMATS, classType).getDateTime();
		}
	}

	public static String apply(Option<Offset> option) {
		Offset offset = option.isEmpty() ? null : option.get();
		return apply(offset);
	}

	@Override
	public String json() {
		return Serialization.write(this, FORMATS);
	}
}
