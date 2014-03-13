package com.sanss.oidd.analyst.dwell;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sanss.oidd.common.io.DwellGroup;

public class UserDwellFormatMapper extends
		Mapper<Text, DwellGroup, Text, NullWritable> {

	protected static final String FIELDS_SEPARATOR = ",";
	private Text outputKey = new Text();

	@Override
	protected void map(Text key, DwellGroup value, Context context)
			throws IOException, InterruptedException {
		if (value.getType().get() == 2) {
			// do something with the location shift group
			return;
		} else {
			int hh0, hh1, factor;
			StringTokenizer token;

			for (int i = (value.getBegin().get() / 3600); i <= (value.getEnd()
					.get() / 3600); i++) {
				if (i * 3600 >= value.getEnd().get()) {
					break;
				}
				// activity value in [1,100]
				hh1 = Math.min(value.getEnd().get(), (i + 1) * 3600);
				hh0 = Math.max(value.getBegin().get(), i * 3600);
				factor = (hh1 - hh0 >= 3599 ? 3600 : hh1 - hh0) * 100 / 3600;

				token = new StringTokenizer(value.getLocs(), "|");
				while (token.hasMoreTokens()) {
					outputKey
							.set(new StringBuilder(key.toString())
									.append(FIELDS_SEPARATOR)
									.append(token.nextToken())
									.append(FIELDS_SEPARATOR)
									.append(value.getDate().toString())
									.append(FIELDS_SEPARATOR).append(getHH(i))
									.append(FIELDS_SEPARATOR).append(factor)
									.toString());
					context.write(outputKey, NullWritable.get());
				}
			}
		}
	}

	private String getHH(int hour) {
		StringBuilder sb = new StringBuilder("00").append(hour);
		return sb.substring(sb.length() - 2);
	}
}
