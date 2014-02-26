package com.sanss.oidd.analyst.lsfilter;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sanss.oidd.common.io.EventInfo;

public class UserLocStateReducer extends
		Reducer<Text, EventInfo, Text, NullWritable> {

	protected static final int CONS_POWER_OFF = 2;
	protected Text outputKey = new Text();

	@Override
	protected void reduce(Text key, Iterable<EventInfo> values, Context context)
			throws IOException, InterruptedException {
		for (EventInfo info : values) {
			outputKey.set(formatter(info));
			context.write(outputKey, NullWritable.get());
		}
	}

	private String formatter(EventInfo info) {
		return new StringBuilder().append(info.getMdn().toString()).append(",")
				.append(info.getTrackDate().toString()).append(",")
				.append(info.getCell().toString()).append(",")
				.append(info.getSector().toString()).append(",")
				.append(info.getEvent().get() != CONS_POWER_OFF ? 1 : 0)
				.toString();
	}
}
