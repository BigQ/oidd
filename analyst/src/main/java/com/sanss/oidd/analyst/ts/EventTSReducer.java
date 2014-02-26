package com.sanss.oidd.analyst.ts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.sanss.oidd.common.io.EventInfo;
import com.sanss.oidd.common.io.EventTSArray;
import com.sanss.oidd.common.io.UserTimePair;

public class EventTSReducer extends
		Reducer<UserTimePair, EventInfo, Text, EventTSArray> {

	private final List<EventInfo> container = new ArrayList<>();

	private EventTSArray array = new EventTSArray();
	private Text outputKey = new Text();
	private String lastImsi = null;

	@Override
	protected void reduce(UserTimePair key, Iterable<EventInfo> values,
			Context context) throws IOException, InterruptedException {
		if (lastImsi == null) {// first invoke
			lastImsi = key.getUser().toString();
		} else if (lastImsi.equals(key.getUser().toString())) {
			// flush the container
			flushData(context);
			// clean the container
			container.clear();
			// mark the new Imsi
			lastImsi = key.getUser().toString();
		}

		for (EventInfo info : values) {
			container.add(info.copy());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		if (lastImsi != null) {
			flushData(context);
		}
	}

	private void flushData(Context context) throws IOException,
			InterruptedException {
		Writable[] arrtemp = new Writable[container.size()];
		array.set(container.toArray(arrtemp));
		outputKey.set(lastImsi);
		context.write(outputKey, array);
	}
}
