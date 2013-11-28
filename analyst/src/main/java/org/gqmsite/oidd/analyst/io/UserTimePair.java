package org.gqmsite.oidd.analyst.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class UserTimePair implements WritableComparable<UserTimePair> {

	private Text mdn;
	private IntWritable diffs;

	public UserTimePair() {
		set(new Text(), new IntWritable());
	}

	public UserTimePair(String mdn, int diffs) {
		set(new Text(mdn), new IntWritable(diffs));
	}

	public UserTimePair(Text mdn, IntWritable diffs) {
		set(mdn, diffs);
	}

	public void set(Text mdn, IntWritable diffs) {
		this.mdn = mdn;
		this.diffs = diffs;
	}

	public Text getMdn() {
		return mdn;
	}

	public IntWritable getDiffs() {
		return diffs;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		mdn.readFields(in);
		diffs.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		mdn.write(out);
		diffs.write(out);
	}

	@Override
	public int compareTo(UserTimePair o) {
		int cmp = mdn.compareTo(o.getMdn());
		if (cmp != 0) {
			return cmp;
		}
		return diffs.compareTo(o.getDiffs());
	}

}
