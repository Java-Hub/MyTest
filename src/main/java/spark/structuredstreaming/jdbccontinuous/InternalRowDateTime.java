package main.java.spark.structuredstreaming.jdbccontinuous;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * @author caik
 * @since 2021/3/17
 */
public class InternalRowDateTime extends InternalRow {
	@Override
	public int numFields() {
		return 0;
	}

	@Override
	public void setNullAt(int i) {

	}

	@Override
	public void update(int i, Object value) {

	}

	@Override
	public InternalRow copy() {
		return null;
	}

	@Override
	public boolean isNullAt(int ordinal) {
		return false;
	}

	@Override
	public boolean getBoolean(int ordinal) {
		return false;
	}

	@Override
	public byte getByte(int ordinal) {
		return 0;
	}

	@Override
	public short getShort(int ordinal) {
		return 0;
	}

	@Override
	public int getInt(int ordinal) {
		return 0;
	}

	@Override
	public long getLong(int ordinal) {
		return 0;
	}

	@Override
	public float getFloat(int ordinal) {
		return 0;
	}

	@Override
	public double getDouble(int ordinal) {
		return 0;
	}

	@Override
	public Decimal getDecimal(int ordinal, int precision, int scale) {
		return null;
	}

	@Override
	public UTF8String getUTF8String(int ordinal) {
		return null;
	}

	@Override
	public byte[] getBinary(int ordinal) {
		return new byte[0];
	}

	@Override
	public CalendarInterval getInterval(int ordinal) {
		return null;
	}

	@Override
	public InternalRow getStruct(int ordinal, int numFields) {
		return null;
	}

	@Override
	public ArrayData getArray(int ordinal) {
		return null;
	}

	@Override
	public MapData getMap(int ordinal) {
		return null;
	}

	@Override
	public Object get(int ordinal, DataType dataType) {
		return null;
	}
}
