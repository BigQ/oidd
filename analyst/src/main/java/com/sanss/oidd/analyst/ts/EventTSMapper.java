package com.sanss.oidd.analyst.ts;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import static com.sanss.oidd.analyst.utils.Common.*;
import com.sanss.oidd.common.io.EventInfo;
import com.sanss.oidd.common.io.UserTimePair;

public class EventTSMapper extends
		Mapper<Text, EventInfo, UserTimePair, EventInfo> {

	private UserTimePair mapOutputKey = new UserTimePair();

	@Override
	protected void map(Text key, EventInfo value, Context context)
			throws IOException, InterruptedException {
		if (value.getMdn().toString().length() != C_V_ILLEGAL_MDN_LEN) {
			// skip illegal IMSI record
			context.getCounter(C_COUNTER_G_SKIPRECORD,
					C_COUNTER_SKIPRECORD_ILLMDN).increment(1);
		} else {
			// generate map output key and value
			mapOutputKey.set(value.getMdn(), value.getTrackDate());
			context.write(mapOutputKey, value);
		}
	}

}
