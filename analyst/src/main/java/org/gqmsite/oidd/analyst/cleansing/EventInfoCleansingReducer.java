package org.gqmsite.oidd.analyst.cleansing;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.gqmsite.oidd.analyst.io.EventInfo;

public class EventInfoCleansingReducer extends
		Reducer<Text, EventInfo, Text, EventInfo> {

	private MultipleOutputs<Text, EventInfo> multipleOutputs;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		multipleOutputs = new MultipleOutputs<Text, EventInfo>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<EventInfo> values, Context context)
			throws IOException, InterruptedException {
		for (EventInfo info : values) {
			multipleOutputs.write(key, info, key.toString().substring(0, 13)
					.replaceAll("\\D", ""));
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		multipleOutputs.close();
	}

}
