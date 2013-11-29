package org.gqmsite.oidd.analyst.ts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.gqmsite.oidd.analyst.io.EventArray;
import org.gqmsite.oidd.analyst.io.EventInfo;
import org.gqmsite.oidd.analyst.io.UserTimePair;

public class UserEventTSReducer extends
		Reducer<UserTimePair, EventInfo, Text, EventArray> {

	private String lastMDN = null;
	private String lastTrackDate = null;
	private String keyWithSalt = null;
	private EventArray array = new EventArray();
	private List<EventInfo> container = new ArrayList<EventInfo>();
	private MultipleOutputs<Text, EventArray> multipleOutputs;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		multipleOutputs = new MultipleOutputs<Text, EventArray>(context);
	}

	@Override
	protected void reduce(UserTimePair key, Iterable<EventInfo> values,
			Context context) throws IOException, InterruptedException {
		// <trackDate>:<MDN>
		keyWithSalt = key.getKey().toString();
		if (lastTrackDate == null) {
			// first invoke
			lastTrackDate = getTrackDateByKeyWithSalt(keyWithSalt);
			lastMDN = getMDNByKeyWithSalt(keyWithSalt);
		} else if (!lastTrackDate
				.equals(getTrackDateByKeyWithSalt(keyWithSalt))
				|| !lastMDN.equals(getMDNByKeyWithSalt(keyWithSalt))) {
			// flush the lastMDN data
			flushData(context);
			// clean the container
			container.clear();
			// mark the new MDN
			lastTrackDate = getTrackDateByKeyWithSalt(keyWithSalt);
			lastMDN = getMDNByKeyWithSalt(keyWithSalt);
		}

		for (EventInfo info : values) {
			container.add(info.copy());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// flush the lastMDN data
		flushData(context);
		// close
		multipleOutputs.close();
	}

	private void flushData(Context context) throws IOException,
			InterruptedException {
		Writable[] arrtemp = new Writable[container.size()];
		array.set(container.toArray(arrtemp));
		multipleOutputs.write(new Text(lastMDN), array, lastTrackDate);
	}

	private String getTrackDateByKeyWithSalt(String key) {
		return key.substring(0, 8);
	}

	private String getMDNByKeyWithSalt(String key) {
		return key.substring(9);
	}
}
