package com.sanss.oidd.analyst.dwell;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sanss.oidd.common.io.DwellActivityItem;
import com.sanss.oidd.common.io.DwellGroup;

public class DwellSplitMapper extends
		Mapper<Text, DwellGroup, Text, DwellActivityItem> {

	private DwellActivityItem mapOutputValue = new DwellActivityItem();

	@Override
	protected void map(Text key, DwellGroup value, Context context)
			throws IOException, InterruptedException {
		if (value.getType().get() == 2) {
			// do something with the location shift group
			return;
		} else {
			mapOutputValue.getDate().set(value.getDate().copyBytes());
			for (int i = (value.getBegin().get() / 3600); i <= (value.getEnd()
					.get() / 3600); i++) {
				if (i * 3600 >= value.getEnd().get()) {
					break;
				}
				// activity value in [1,100]
				mapOutputValue.getAc().set(
						(value.getEnd().get() - i * 3600 > 3600 ? 3600 : value
								.getEnd().get() - i * 3600) * 100 / 3600);
				mapOutputValue.getHh().set(getHH(i));
				StringTokenizer token = new StringTokenizer(value.getLocs(),
						"|");
				while (token.hasMoreTokens()) {
					mapOutputValue.getLoc().set(token.nextToken());
					context.write(key, mapOutputValue);
				}
			}
		}
	}

	private String getHH(int hour) {
		StringBuilder sb = new StringBuilder("00").append(hour);
		return sb.substring(sb.length() - 2);
	}
}
