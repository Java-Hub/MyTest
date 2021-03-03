package main.java.spark.structuredstreaming;

import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Streaming监听器
 * @author caik
 * @since 2021/2/24
 */
public class StreamingListener extends StreamingQueryListener {

	private Map<UUID, Long> map = new HashMap<>();

	@Override
	public void onQueryStarted(QueryStartedEvent event) {

	}

	@Override
	public void onQueryProgress(QueryProgressEvent event) {
		StreamingQueryProgress progress = event.progress();
		long rows = progress.numInputRows();
		if (rows > 0) {
			System.out.println(progress.name() + rows);
			UUID uuid = progress.id();
			long count = map.containsKey(uuid) ? map.get(uuid) + rows : rows;
			map.put(uuid, count);
			System.out.println("累计：" + count);
		}
	}

	@Override
	public void onQueryTerminated(QueryTerminatedEvent event) {

	}
}
