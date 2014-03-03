package com.sanss.oidd.common.io;

import org.apache.hadoop.io.ArrayWritable;

public class LocStayArray extends ArrayWritable {

	public LocStayArray() {
		super(LocStayInfo.class);
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
