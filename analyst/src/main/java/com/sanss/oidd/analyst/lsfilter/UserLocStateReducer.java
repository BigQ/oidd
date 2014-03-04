package com.sanss.oidd.analyst.lsfilter;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.sanss.oidd.analyst.utils.Common;
import com.sanss.oidd.common.io.EventInfo;
import com.sanss.oidd.common.io.EventTSArray;

public class UserLocStateReducer extends
		Reducer<Text, EventTSArray, Text, NullWritable> {

	protected Text outputKey = new Text();

	@Override
	protected void reduce(Text key, Iterable<EventTSArray> values,
			Context context) throws IOException, InterruptedException {
		for (EventTSArray array : values) {
			for (Writable w : array.get()) {
				EventInfo info = (EventInfo) w;
				outputKey.set(formatter(info));
				context.write(outputKey, NullWritable.get());
			}

		}
	}

	private String formatter(EventInfo info) {
		return new StringBuilder().append(info.getMdn().toString()).append(",")
				.append(info.getTrackDate().toString()).append(",")
				.append(info.getCell().toString()).append(",")
				.append(info.getSector().toString()).append(",")
				.append(Common.convertEvent2State(info.getEvent().get()))
				.toString();
	}
}
