package main.java.spark.structuredstreaming.jdbccontinuous;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.JulianFields;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Spark字段结构工具类
 * @author caik
 * @since 2021年1月5日
 */
public final class FieldUtils {

	private static final String DECIMAL_TYPE_NAME = getTypeName(DecimalType.USER_DEFAULT());

	/**
	 * 获取数据类型名称
	 * @see DataType#typeName()
	 */
	public static String getTypeName(DataType dataType) {
		if (dataType == null) {
			return getTypeName(DataTypes.NullType);
		}
		String typeName = dataType.getClass().getSimpleName();
		typeName = StringUtils.removeEndIgnoreCase(typeName, "$");
		typeName = StringUtils.removeEndIgnoreCase(typeName, "Type");
		typeName = StringUtils.removeEndIgnoreCase(typeName, "UDT");
		return typeName.toUpperCase(Locale.ROOT);
	}

	public static boolean isDecimal(DataType dataType) {
		return DECIMAL_TYPE_NAME.equalsIgnoreCase(getTypeName(dataType));
	}

	/**
	 * 将Row转换为String数组
	 * 如果Row中类型存在日期型，则需要使用{@link FieldUtils#getValuesFromRow(Row, String)},并指定日期格式
	 */
	public static String[] getValuesFromRow(Row row) {
		return getValuesFromRow(row, null);
	}

	/**
	 * 将Row转换为String数组。如果Row中数据包含日期类型，需要指定日期格式
	 */
	public static String[] getValuesFromRow(Row row, String dateFormat) {
		int length = row.length();
		String[] values = new String[length];
		for (int i = 0; i < length; i++) {
			values[i] = getAsString(row, i, dateFormat);
		}
		return values;
	}

	/**
	 * 将Row中第i列的数据以字符串格式取出来
	 * 如果该列是日期格式，则需要使用{@link FieldUtils#getAsString(Row, int, String)}方法，并指定日期格式
	 */
	public static String getAsString(Row row, int i) {
		return getAsString(row, i, null);
	}

	/**
	 * 将Row中第i列的数据以字符串格式取出来。如果是日期类型，需要指定日期格式
	 */
	public static String getAsString(Row row, int i, String dateFormat) {
		StructType schema = row.schema();
		StructField structField = schema.apply(i);
		DataType dataType = structField.dataType();
		if (dataType instanceof ByteType) {
			return String.valueOf(row.getByte(i));
		} else if (dataType instanceof ShortType) {
			return String.valueOf(row.getShort(i));
		} else if (dataType instanceof IntegerType) {
			return String.valueOf(row.getInt(i));
		} else if (dataType instanceof LongType) {
			return String.valueOf(row.getLong(i));
		} else if (dataType instanceof FloatType) {
			return String.valueOf(row.getFloat(i));
		} else if (dataType instanceof DoubleType) {
			return String.valueOf(row.getDouble(i));
		} else if (dataType instanceof DecimalType) {
			return row.getDecimal(i).toString();
		} else if (dataType instanceof StringType) {
			return row.getString(i);
		} else if (dataType instanceof BinaryType) {
			return new String((byte[]) row.get(i), StandardCharsets.UTF_8);
		} else if (dataType instanceof BooleanType) {
			return String.valueOf(row.getBoolean(i));
		} else if (dataType instanceof TimestampType || dataType instanceof DateType) {
			SimpleDateFormat format = new SimpleDateFormat(dateFormat);
			return format.format(row.getTimestamp(i));
		}
		return String.valueOf(row.get(i));
	}

	/**
	 * 根据StructType将String数组转换为Row
	 * 如果数据中含有日期类型，需要使用{@link FieldUtils#getRowFromValues(String[], StructType, String)}，并指定日期格式
	 */
	public static Row getRowFromValues(String[] values, StructType structType) {
		if (values == null) {
			return RowFactory.create();
		}
		if (structType == null || structType.isEmpty()) {
			return RowFactory.create(values);
		}
		int length = structType.length();
		Object[] data = new Object[length];
		int valueSize = values.length;
		for (int i = 0; i < length; i++) {
			data[i] = i >= valueSize ? null : getWithType(values[i], structType.apply(i).dataType());
		}
		return new GenericRowWithSchema(data, structType);
	}

	/**
	 * 根据StructType将String数组转换为Row，如果数据中含有日期类型，需要指定日期格式
	 */
	public static Row getRowFromValues(String[] values, StructType structType, String dateFormat) throws ParseException {
		if (values == null) {
			return RowFactory.create();
		}
		if (structType == null || structType.isEmpty()) {
			return RowFactory.create(values);
		}
		int length = structType.length();
		Object[] data = new Object[length];
		int valueSize = values.length;
		for (int i = 0; i < length; i++) {
			data[i] = i >= valueSize ? null : getWithType(values[i], structType.apply(i).dataType(), dateFormat);
		}
		return new GenericRowWithSchema(data, structType);
	}

	public static Object[] getWithType(ResultSet rs, StructType schema) throws SQLException {
		int length = schema.length();
		Object[] data = new Object[length];
		for (int i = 0; i < length; i++) {
			StructField structField = schema.apply(i);
			DataType dataType = structField.dataType();
			String name = structField.name();
			if (dataType instanceof ByteType) {
				data[i] = rs.getByte(name);
			} else if (dataType instanceof ShortType) {
				data[i] = rs.getShort(name);
			} else if (dataType instanceof IntegerType) {
				data[i] = rs.getInt(name);
			} else if (dataType instanceof LongType) {
				data[i] = rs.getLong(name);
			} else if (dataType instanceof FloatType) {
				data[i] = rs.getFloat(name);
			} else if (dataType instanceof DoubleType) {
				data[i] = rs.getFloat(name);
			} else if (dataType instanceof DecimalType) {
				data[i] = Decimal.apply(rs.getBigDecimal(name));
			} else if (dataType instanceof StringType) {
				data[i] = UTF8String.fromString(rs.getString(name));
			} else if (dataType instanceof BinaryType) {
				data[i] = rs.getBytes(name);
			} else if (dataType instanceof BooleanType) {
				data[i] = rs.getBoolean(name);
			} else if(dataType instanceof DateType){
				java.sql.Date date = rs.getDate(name);
				data[i] = date.getTime() / (1000 * 60 * 60 * 24);
			} else if (dataType instanceof TimestampType || dataType instanceof DateType) {
				Timestamp timestamp = rs.getTimestamp(name);
				data[i] = timestamp.getTime() * 1000;
			}
		}
		return data;
	}

	/**
	 * 将String转换为指定类型的数据
	 * 如果是日期类型,则需要使用{@link FieldUtils#getWithType(String, DataType, String)}，并指定日期格式
	 */
	public static Object getWithType(String value, DataType dataType) {
		try {
			return getWithType(value, dataType, null);
		} catch (ParseException e) {
			return value;
		}
	}

	/**
	 * 将String转换为指定类型的数据，如果是日期类型，则需要指定日期格式
	 */
	public static Object getWithType(String value, DataType dataType, String dateFormat) throws ParseException {
		if (value == null) {
			return null;
		}
		if (dataType instanceof ByteType) {
			return Byte.valueOf(value);
		} else if (dataType instanceof ShortType) {
			return Short.valueOf(value);
		} else if (dataType instanceof IntegerType) {
			return Integer.valueOf(value);
		} else if (dataType instanceof LongType) {
			return Long.valueOf(value);
		} else if (dataType instanceof FloatType) {
			return Float.valueOf(value);
		} else if (dataType instanceof DoubleType) {
			return Double.valueOf(value);
		} else if (dataType instanceof DecimalType) {
			return new BigDecimal(value.toCharArray());
		} else if (dataType instanceof StringType) {
			return value;
		} else if (dataType instanceof BinaryType) {
			return value.getBytes(StandardCharsets.UTF_8);
		} else if (dataType instanceof BooleanType) {
			return Boolean.valueOf(value);
		} else if (dataType instanceof TimestampType || dataType instanceof DateType) {
			SimpleDateFormat format = new SimpleDateFormat(dateFormat);
			return format.parse(value);
		}
		return value;
	}

	/**
	 * 计算时间，共12位
	 * 前8位是今天00:00:00以来的纳秒数，后四位是儒略日
	 * @param date
	 * @return
	 */
	public static ByteBuffer getDateTime(Date date) {
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		calendar.setTime(date);

		//计算纳秒
		long nsOneHour = TimeUnit.HOURS.toNanos(1);
		long nsOneMinute = TimeUnit.MINUTES.toNanos(1);
		long nsOneSecond = TimeUnit.SECONDS.toNanos(1);
		long nanos = calendar.get(Calendar.HOUR) * nsOneHour + calendar.get(Calendar.MINUTE) + nsOneMinute + calendar.get(Calendar.SECOND) * nsOneSecond;

		//计算儒略日
		LocalDate localDate = LocalDate.of(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH));
		long day = JulianFields.JULIAN_DAY.getFrom(localDate);

		ByteBuffer buffer = ByteBuffer.allocate(12);
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		buffer.putLong(nanos);
		buffer.putInt((int) day);
		buffer.flip();
		return buffer;
	}

	public static StructType getSchema(ResultSetMetaData metaData) throws SQLException {
		StructType schema = new StructType();
		int count = metaData.getColumnCount();
		for (int i = 1; i <= count; i++) {
			int type = metaData.getColumnType(i);
			int precision = metaData.getPrecision(i);
			int scale = metaData.getScale(i);
			DataType dataType = getTypeFromSQLType(type, precision, scale);
			String name = metaData.getColumnName(i);
			schema = schema.add(name, dataType);
		}
		return schema;
	}

	/**
	 * DataTypes.IntegerType
	 * DataTypes.LongType
	 * DataTypes.createDecimalType(length, scale)
	 * DataTypes.StringType
	 * DataTypes.TimestampType
	 * DataTypes.BooleanType
	 * @param type
	 * @param length
	 * @param scale
	 * @return
	 */
	public static DataType getTypeFromSQLType(int type, int length, int scale) {
		switch (type) {
			case Types.BIT:
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
				return DataTypes.IntegerType;
			case Types.BIGINT:
				return DataTypes.LongType;
			case Types.FLOAT:
			case Types.DOUBLE:
			case Types.NUMERIC:
			case Types.DECIMAL:
				return DataTypes.createDecimalType(length, scale);
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.CLOB:
			case Types.NCHAR:
			case Types.NVARCHAR:
			case Types.LONGNVARCHAR:
			case Types.NCLOB:
				return DataTypes.StringType;
			case Types.DATE:
			case Types.TIME:
			case Types.TIMESTAMP:
				return DataTypes.TimestampType;
			case Types.BOOLEAN:
				return DataTypes.BooleanType;
			default:
				return DataTypes.StringType;
		}
	}

	private FieldUtils() {

	}

}
