package com.sanss.oidd.analyst.lsfilter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.sanss.oidd.common.io.EventInfo;
import com.sanss.oidd.common.io.EventTSArray;
import com.sanss.oidd.common.io.UserTimePair;

import static com.sanss.oidd.analyst.utils.Common.*;

public class UserLocStateMapper extends
		Mapper<Text, EventTSArray, UserTimePair, EventInfo> {

	private UserTimePair mapOutputKey = new UserTimePair();

	@Override
	protected void map(Text key, EventTSArray value, Context context)
			throws IOException, InterruptedException {
		EventInfo info;
		String lastCell = null;
		int lastSector = -1;
		int lastState = -1;

		for (Writable w : value.get()) {
			info = (EventInfo) w;

			if (info.getMdn().toString().equals(C_V_UNKNOWN_MDN)) {
				// skip illegal MDN
				context.getCounter(C_COUNTER_G_SKIPRECORD,
						C_COUNTER_SKIPRECORD_ILLMDN).increment(1);
			} else {
				if (lastCell != null
						&& info.getCell().toString().equals(lastCell)
						&& info.getSector().get() == lastSector
						&& convertEvent2State(info.getEvent().get()) == lastState) {
					// skip duplicated items (location & state)
					context.getCounter(C_COUNTER_G_SKIPRECORD,
							C_COUNTER_SKIPRECORD_DUP).increment(1);
				} else {
					// set the map output key and value
					mapOutputKey.set(info.getMdn(), info.getTrackDate());
					context.write(mapOutputKey, info);
				}

				// mark the lastCell, lastSector, lastState
				lastCell = info.getCell().toString();
				lastSector = info.getSector().get();
				lastState = convertEvent2State(info.getEvent().get());
			}

		}

	}
}
