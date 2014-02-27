package com.sanss.oidd.analyst.lsfilter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sanss.oidd.common.io.EventInfo;
import com.sanss.oidd.common.io.EventTSArray;

public class UserLocStateMapper extends
		Mapper<Text, EventTSArray, Text, EventInfo> {

	protected static final String UNKNOWN_MDN = "0";
	protected static final String SKIP_RECORD_COUNTER_GROUP = "SKIP_RECORD";
	protected static final String SKIP_RECORD_DUP_COUNTER = "duplicated";
	protected static final String SKIP_RECORD_ILL_COUNTER = "illegal";

	@Override
	protected void map(Text key, EventTSArray value, Context context)
			throws IOException, InterruptedException {
		EventInfo info;
		String lastCell = null;
		int lastSector = 0;

		for (int i = 0; i < value.get().length; i++) {
			info = (EventInfo) (value.get()[i]);

			if (info.getMdn().toString().equals(UNKNOWN_MDN)) {
				// skip illegal MDN
				context.getCounter(SKIP_RECORD_COUNTER_GROUP,
						SKIP_RECORD_ILL_COUNTER).increment(1);
			} else {
				if (lastCell != null
						&& info.getCell().toString().equals(lastCell)
						&& info.getSector().get() == lastSector) {
					// skip duplicated items
					context.getCounter(SKIP_RECORD_COUNTER_GROUP,
							SKIP_RECORD_DUP_COUNTER).increment(1);
				} else {
					context.write(info.getMdn(), info);
				}

				lastCell = info.getCell().toString();
				lastSector = info.getSector().get();
			}

		}

	}

}
