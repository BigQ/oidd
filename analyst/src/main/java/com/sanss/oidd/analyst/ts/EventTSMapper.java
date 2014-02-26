package com.sanss.oidd.analyst.ts;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sanss.oidd.common.io.EventInfo;
import com.sanss.oidd.common.io.UserTimePair;

public class EventTSMapper extends
		Mapper<Text, EventInfo, UserTimePair, EventInfo> {

	protected static final String ILLEGAL_IMSI = "0";
	protected static final String SKIP_COUNTER_GROUP = "SKIP_ILLEGAL_RECORDS";
	protected static final String SKIP_ILLEGALIMSI_COUNTER = "ILLEGAL_IMSI";

	private UserTimePair mapOutputKey = new UserTimePair();

	@Override
	protected void map(Text key, EventInfo value, Context context)
			throws IOException, InterruptedException {
		if (value.getImsi().toString().equals(ILLEGAL_IMSI)) {
			context.getCounter(SKIP_COUNTER_GROUP, SKIP_ILLEGALIMSI_COUNTER)
					.increment(1);
		} else {
			mapOutputKey.set(value.getImsi(), value.getTrackDate());
			context.write(mapOutputKey, value);
		}
	}

}
