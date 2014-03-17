package com.sanss.oidd.analyst.active;

import static com.sanss.oidd.analyst.utils.Common.C_COUNTER_G_SKIPRECORD;
import static com.sanss.oidd.analyst.utils.Common.C_COUNTER_SKIPRECORD_ERROR;
import static com.sanss.oidd.analyst.utils.Common.getSecondsInDay;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.sanss.oidd.analyst.utils.Common;
import com.sanss.oidd.common.io.EventInfo;
import com.sanss.oidd.common.io.EventTSArray;

public class UserActiveCalcMapper extends
		Mapper<Text, EventTSArray, Text, NullWritable> {

	protected static final String FIELDS_SEPARATOR = ",";
	private Text mapOutputKey = new Text();

	@Override
	protected void map(Text key, EventTSArray value, Context context)
			throws IOException, InterruptedException {
		int online, m1, m2, c1, c2, x1, nbe;
		int lastDiffs, diffs;
		EventInfo info = null;
		boolean lastOffline = false;
		m1 = m2 = c1 = c2 = x1 = nbe = online = 0;
		lastDiffs = diffs = 0;

		for (Writable w : value.get()) {
			info = (EventInfo) w;

			try {
				diffs = getSecondsInDay(info.getTrackDate().toString());
				if (!lastOffline) {
					online = online
							+ Math.min(diffs - lastDiffs,
									Common.C_V_EVENT_MAX_INTERVAL);
				}
				lastDiffs = diffs;
			} catch (Exception ex) {
				context.getCounter(C_COUNTER_G_SKIPRECORD,
						C_COUNTER_SKIPRECORD_ERROR).increment(1);
				continue;
			}

			switch (info.getEvent().get()) {
			case Common.C_EVENT_SMSSEND:
				m1++;
				break;
			case Common.C_EVENT_SMSRECEIVE:
				m2++;
				break;
			case Common.C_EVENT_CALLING:
				c1++;
				break;
			case Common.C_EVENT_CALLED:
				c2++;
				break;
			case Common.C_EVENT_X1ONLINE:
				x1++;
				break;
			default:
				nbe++;
			}

			if (info.getEvent().get() == Common.C_EVENT_POWEROFF) {
				lastOffline = true;
			} else {
				lastOffline = false;
			}
		}
		if (info != null) {
			mapOutputKey.set(formatKey(info, online, c1, c2, m1, m2, x1, nbe));
			context.write(mapOutputKey, NullWritable.get());
		}
	}

	private String formatKey(EventInfo info, int online, int c1, int c2,
			int m1, int m2, int x1, int nbe) {
		return new StringBuilder(info.getMdn().toString())
				.append(FIELDS_SEPARATOR).append(info.getImsi().toString())
				.append(FIELDS_SEPARATOR)
				.append(Math.round(online * 1.0f / 60))
				.append(FIELDS_SEPARATOR).append(c1).append(FIELDS_SEPARATOR)
				.append(c2).append(FIELDS_SEPARATOR).append(m1)
				.append(FIELDS_SEPARATOR).append(m2).append(FIELDS_SEPARATOR)
				.append(x1).append(FIELDS_SEPARATOR).append(nbe).toString();
	}
}
