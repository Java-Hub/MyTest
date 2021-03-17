package main.java.spark.structuredstreaming.jdbccontinuous;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.execution.BufferedRowIterator;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.Iterator;

import java.sql.Timestamp;

/**
 * @author caik
 * @since 2021/3/12
 */
public class Test {


	@org.junit.Test
	public void test1(){
		JdbcPartitionOffset offset = new JdbcPartitionOffset(new Timestamp(0));
		System.out.println(offset.getAsString());
	}

	public Object generate(Object[] references) {
		return new GeneratedIteratorForCodegenStage1(references);
	}

	/*wsc_codegenStageId*/
	final class GeneratedIteratorForCodegenStage1 extends BufferedRowIterator {
		private Object[] references;

		private Iterator[] inputs;

		private UnsafeRowWriter[] unsafeRowWriters = new UnsafeRowWriter[1];

		private Iterator[] iterators = new Iterator[1];

		public GeneratedIteratorForCodegenStage1(Object[] references) {
			this.references = references;
		}

		public void init(int index, scala.collection.Iterator[] inputs) {
			partitionIndex = index;
			this.inputs = inputs;
			iterators[0] = inputs[0];
			unsafeRowWriters[0] = new UnsafeRowWriter(3, 64);

		}

		protected void processNext() throws java.io.IOException {
			while (iterators[0].hasNext()) {

				InternalRow internalRow = (InternalRow) iterators[0].next();
				((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);

				boolean nullAt0 = internalRow.isNullAt(0);
				UTF8String utf8String = nullAt0 ? null : (internalRow.getUTF8String(0));

				boolean nullAt1 = internalRow.isNullAt(1);
				Decimal decimal = nullAt1 ? null : (internalRow.getDecimal(1, 38, 10));

				boolean nullAt2 = internalRow.isNullAt(2);
				long datasourcev2scan_value_2 = nullAt2 ? -1L : (internalRow.getLong(2));
				unsafeRowWriters[0].reset();

				unsafeRowWriters[0].zeroOutNullBytes();

				if (nullAt0) {
					unsafeRowWriters[0].setNullAt(0);
				} else {
					unsafeRowWriters[0].write(0, utf8String);
				}

				if (nullAt1) {
					unsafeRowWriters[0].write(1, (Decimal) null, 38, 10);
				} else {
					unsafeRowWriters[0].write(1, decimal, 38, 10);
				}

				if (nullAt2) {
					unsafeRowWriters[0].setNullAt(2);
				} else {
					unsafeRowWriters[0].write(2, datasourcev2scan_value_2);
				}
				append((unsafeRowWriters[0].getRow()));
				if (shouldStop())
					return;
			}
		}

	}

}


