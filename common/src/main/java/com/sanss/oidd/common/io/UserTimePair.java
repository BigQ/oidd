package com.sanss.oidd.common.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class UserTimePair implements WritableComparable<UserTimePair> {

	private Text user;
	private Text time;

	public UserTimePair() {
		set(new Text(), new Text());
	}

	public UserTimePair(Text user, Text time) {
		set(user, time);
	}

	public UserTimePair(String id, String timeExp) {
		set(new Text(id), new Text(timeExp));
	}

	public void set(Text user, Text time) {
		this.user = user;
		this.time = time;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		user.write(out);
		time.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		user.readFields(in);
		time.readFields(in);
	}

	@Override
	public int compareTo(UserTimePair o) {
		int cmp = user.compareTo(o.getUser());
		if (cmp != 0) {
			return cmp;
		}
		return time.compareTo(o.getTime());
	}

	public Text getUser() {
		return user;
	}

	public Text getTime() {
		return time;
	}

}
