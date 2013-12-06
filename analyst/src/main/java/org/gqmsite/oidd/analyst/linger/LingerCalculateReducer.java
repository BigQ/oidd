package org.gqmsite.oidd.analyst.linger;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.gqmsite.oidd.analyst.io.LingerPair;

public class LingerCalculateReducer extends
		Reducer<LingerPair, IntWritable, LingerPair, IntWritable> {

	@Override
	protected void reduce(LingerPair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int summary = 0;
		for (IntWritable value : values) {
			summary += value.get();
		}
		context.write(key, new IntWritable(summary));
	}

}
