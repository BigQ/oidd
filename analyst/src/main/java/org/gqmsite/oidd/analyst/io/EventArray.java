package org.gqmsite.oidd.analyst.io;

import org.apache.hadoop.io.ArrayWritable;

public class EventArray extends ArrayWritable {

	public EventArray() {
		super(EventInfo.class);
	}

}
