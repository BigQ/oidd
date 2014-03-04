package com.sanss.oidd.analyst.lsfilter;

import static com.sanss.oidd.analyst.utils.Common.C_COUNTER_G_SKIPRECORD;
import static com.sanss.oidd.analyst.utils.Common.C_COUNTER_SKIPRECORD_DUP;
import static com.sanss.oidd.analyst.utils.Common.C_COUNTER_SKIPRECORD_ILLMDN;
import static com.sanss.oidd.analyst.utils.Common.C_V_ILLEGAL_MDN_LEN;
import static com.sanss.oidd.analyst.utils.Common.convertEvent2State;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.sanss.oidd.common.io.EventInfo;
import com.sanss.oidd.common.io.EventTSArray;

public class UserLocStateMapper extends
		Mapper<Text, EventTSArray, Text, EventTSArray> {

	private Text mapOutputKey = new Text();
	private EventTSArray mapOutputValue = new EventTSArray();
	private final List<EventInfo> container = new ArrayList<>();

	@Override
	protected void map(Text key, EventTSArray value, Context context)
			throws IOException, InterruptedException {
		EventInfo info;
		String lastCell = null;
		int lastSector = -1;
		int lastState = -1;

		for (Writable w : value.get()) {
			info = (EventInfo) w;

			if (info.getMdn().toString().length()!=C_V_ILLEGAL_MDN_LEN) {
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
					mapOutputKey.set(info.getMdn().toString());
					container.add(info);
				}

				// mark the lastCell, lastSector, lastState
				lastCell = info.getCell().toString();
				lastSector = info.getSector().get();
				lastState = convertEvent2State(info.getEvent().get());
			}

		}
		
		if(container.size() > 0){
			Writable[] arr = new Writable[container.size()];
			mapOutputValue.set(container.toArray(arr));
			context.write(mapOutputKey, mapOutputValue);
			container.clear();
		}
	}
}
