package org.gqmsite.oidd.analyst.extract.ts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.gqmsite.oidd.analyst.io.EventArray;
import org.gqmsite.oidd.analyst.io.EventInfo;
import org.gqmsite.oidd.analyst.io.UserTimePair;

public class UserEventTSReducer extends
		Reducer<UserTimePair, EventInfo, Text, EventArray> {

	private String preMdn = null;
	private EventArray array = new EventArray();
	private List<EventInfo> container = new ArrayList<EventInfo>();

	@Override
	protected void reduce(UserTimePair key, Iterable<EventInfo> values,
			Context context) throws IOException, InterruptedException {
		if (preMdn == null) {
			preMdn = key.getMdn().toString();
		} else if (!preMdn.equals(key.getMdn().toString())) {
			preMdn = key.getMdn().toString();
			context.write(key.getMdn(), array);
		}

		for (EventInfo info : values) {
			container.add(info.copy());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		context.write(new Text(preMdn), array);
	}

}
