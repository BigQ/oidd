package org.gqmsite.oidd.analyst.cleansing;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EventInfoCleansingReducer extends
		Reducer<Text, EventInfo, Text, EventInfo> {

	@Override
	protected void reduce(Text key, Iterable<EventInfo> values, Context context)
			throws IOException, InterruptedException {
		for (EventInfo info : values) {
			context.write(key, info);
		}
	}

}
