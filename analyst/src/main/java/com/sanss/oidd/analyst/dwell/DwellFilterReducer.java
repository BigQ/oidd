package com.sanss.oidd.analyst.dwell;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sanss.oidd.common.io.DwellActivityItem;

public class DwellFilterReducer extends
		Reducer<Text, DwellActivityItem, Text, Text> {

	protected static final String MEASURE_DAILY_THRESHOLD = "oidd.dwell.measure.daily.min";
	protected static final String MEASURE_DAYS_THRESHOLD = "oidd.dwell.measure.days.min";

	private int dailyAcMin;
	private int daysCountMin;

	private HashMap<String, IntWritable> dailyCounter = new HashMap<>();

	private Text outputKey = new Text();
	private Text outputValue = new Text();

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
		int min = Integer.MAX_VALUE, max = 0, total = 0, count = 0;

		for (String day : dailyCounter.keySet()) {
			if (dailyCounter.get(day).get() >= dailyAcMin) {
				count++;
				min = Math.min(min, dailyCounter.get(day).get());
				max = Math.max(max, dailyCounter.get(day).get());
				total += dailyCounter.get(day).get();
			}
		}

		if (count >= daysCountMin) {
			StringTokenizer token = new StringTokenizer(key.toString(), ",");
			outputKey.set(token.nextToken());
			StringBuilder sb = new StringBuilder();
			sb.append(token.nextToken()).append(",").append(count).append(",")
					.append(total).append(",").append(min).append(",")
					.append(max);
			outputValue.set(sb.toString());
			context.write(outputKey, outputValue);
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		dailyAcMin = context.getConfiguration().getInt(MEASURE_DAILY_THRESHOLD,
				0);
		daysCountMin = context.getConfiguration().getInt(
				MEASURE_DAYS_THRESHOLD, 0);
	}

}
