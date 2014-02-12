package org.gqmsite.oidd.analyst.bd;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class BusinessAreaAccessReducer extends
		Reducer<Text, IntWritable, Text, NullWritable> {

	private MultipleOutputs<Text, NullWritable> multipleOutputs;

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		multipleOutputs.close();
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		multipleOutputs = new MultipleOutputs<>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		StringTokenizer token = new StringTokenizer(key.toString(), "@");
		if (token.countTokens() != 2) {
			return;
		}
		String area = token.nextToken();
		String mdn = token.nextToken();

		int min = Integer.MAX_VALUE;
		int max = 0;
		int sum = 0;
		int count = 0;

		for (IntWritable linger : values) {
			sum += linger.get();
			min = Math.min(min, linger.get());
			max = Math.max(max, linger.get());
			count++;
		}

		multipleOutputs.write(
				new Text(resultFormat(area, mdn, count, sum, min, max)),
				NullWritable.get(), area);
	}

	private String resultFormat(String area, String mdn, int count, int sum,
			int min, int max) {
		return new StringBuilder().append(mdn).append(",").append(count)
				.append(",").append(sum).append(",").append(min).append(",")
				.append(max).toString();
	}
}
