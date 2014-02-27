package com.sanss.oidd.common.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UserTimeGroupComparator extends WritableComparator {

	protected UserTimeGroupComparator() {
		super(UserTimePair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		UserTimePair p1 = (UserTimePair) a;
		UserTimePair p2 = (UserTimePair) b;
		return p1.getUser().compareTo(p2.getUser());
	}
}
