package com.sanss.oidd.analyst.dwell;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sanss.oidd.common.io.DwellActivityItem;

public class DwellFilterCombiner extends
		Reducer<Text, DwellActivityItem, Text, DwellActivityItem> {

	private DwellActivityItem mapOutputValue = new DwellActivityItem();
	private HashMap<String, IntWritable> dailyCounter = new HashMap<>();

	@Override
	protected void reduce(Text key, Iterable<DwellActivityItem> values,
			Context context) throws IOException, InterruptedException {

		dailyCounter.clear();

		for (DwellActivityItem item : values) {
			if (dailyCounter.containsKey(item.getDate().toString())) {
				dailyCounter.get(item.getDate().toString()).set(
						dailyCounter.get(item.getDate().toString()).get()
								+ item.getAc().get());
			} else {
				dailyCounter.put(item.getDate().toString(), new IntWritable(
						item.getAc().get()));
			}
		}

		for (String day : dailyCounter.keySet()) {
			mapOutputValue.getDate().set(day);
			mapOutputValue.getAc().set(dailyCounter.get(day).get());
			context.write(key, mapOutputValue);
		}

	}
}
