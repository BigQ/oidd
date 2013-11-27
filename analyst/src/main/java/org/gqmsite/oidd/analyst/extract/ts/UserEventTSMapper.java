package org.gqmsite.oidd.analyst.extract.ts;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.gqmsite.oidd.analyst.io.EventInfo;
import org.gqmsite.oidd.analyst.io.UserTimePair;

public class UserEventTSMapper extends
		Mapper<Text, EventInfo, UserTimePair, EventInfo> {

	private final UserTimePair pairKey = new UserTimePair();

	@Override
	protected void map(Text key, EventInfo value, Context context)
			throws IOException, InterruptedException {
		pairKey.set(value.getMdn(), value.getTrackTime());
		context.write(pairKey, value);
	}

}
