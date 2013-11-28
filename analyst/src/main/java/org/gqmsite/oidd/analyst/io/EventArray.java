package org.gqmsite.oidd.analyst.io;

import org.apache.hadoop.io.ArrayWritable;

public class EventArray extends ArrayWritable {

	public EventArray() {
		super(EventInfo.class);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (String s : toStrings()) {
			sb.append(s).append("\n");
		}
		return sb.toString();
	}

}
