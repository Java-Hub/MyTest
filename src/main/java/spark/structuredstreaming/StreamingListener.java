package main.java.spark.structuredstreaming;

import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;

/**
 * Streaming监听器
 * @author caik
 * @since 2021/2/24
 */
public class StreamingListener extends StreamingQueryListener {

	private long size;

	@Override
	public void onQueryStarted(QueryStartedEvent event) {

	}

	@Override
	public void onQueryProgress(QueryProgressEvent event) {
		StreamingQueryProgress progress = event.progress();
		long rows = progress.numInputRows();
		size += rows;
		if(rows > 0){
			System.out.println("本次数据量：" + rows);
			System.out.println("累计数据量：" + size);
		}
	}

	@Override
	public void onQueryTerminated(QueryTerminatedEvent event) {

	}
}
