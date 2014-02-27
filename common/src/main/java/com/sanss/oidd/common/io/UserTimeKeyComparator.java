package com.sanss.oidd.common.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UserTimeKeyComparator extends WritableComparator {

	protected UserTimeKeyComparator() {
		super(UserTimePair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		UserTimePair p1 = (UserTimePair) a;
		UserTimePair p2 = (UserTimePair) b;
		int cmp = p1.getUser().compareTo(p2.getUser());
		if (cmp != 0) {
			return cmp;
		}
		return p1.getTime().compareTo(p2.getTime());
	}
}
