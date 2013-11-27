package org.gqmsite.oidd.analyst.extract.ts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.gqmsite.oidd.analyst.io.EventArray;
import org.gqmsite.oidd.analyst.io.EventInfo;
import org.gqmsite.oidd.analyst.io.UserTimePair;

public class UserEventTSReducer extends
		Reducer<UserTimePair, EventInfo, Text, EventArray> {

	private String preMDN = null;
	private EventArray array = new EventArray();
	private List<EventInfo> container = new ArrayList<EventInfo>();

	@Override
	protected void reduce(UserTimePair key, Iterable<EventInfo> values,
			Context context) throws IOException, InterruptedException {
		if (preMDN == null) {
			// first invoke
			preMDN = key.getMdn().toString();
		} else if (!preMDN.equals(key.getMdn().toString())) {
			// flush the pre-MDN data
			flushData(context);
			// clean the container
			container.clear();
			// mark the new MDN
			preMDN = key.getMdn().toString();
		}

		for (EventInfo info : values) {
			container.add(info.copy());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// flush the pre-MDN data
		flushData(context);
	}

	private void flushData(Context context) throws IOException,
			InterruptedException {
		Writable[] arrtemp = new Writable[container.size()];
		array.set(container.toArray(arrtemp));
		context.write(new Text(preMDN), array);
	}
}
