package org.gqmsite.oidd.analyst.ts;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.gqmsite.oidd.analyst.io.EventInfo;
import org.gqmsite.oidd.analyst.io.UserTimePair;

public class UserEventTSMapper extends
		Mapper<Text, EventInfo, UserTimePair, EventInfo> {

	private final UserTimePair pairKey = new UserTimePair();
	private final StringBuffer keyWithSalt = new StringBuffer();

	@Override
	protected void map(Text key, EventInfo value, Context context)
			throws IOException, InterruptedException {
		keyWithSalt.setLength(0);
		// key pattern <trackDate>:<MDN>
		keyWithSalt.append(value.getTrackDate().toString()).append(":")
				.append(key.toString());
		pairKey.getKey().set(keyWithSalt.toString());
		pairKey.getDiffs().set(value.getDiffs().get());
		context.write(pairKey, value);
	}

}
