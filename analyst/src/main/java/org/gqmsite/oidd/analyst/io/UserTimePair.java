package org.gqmsite.oidd.analyst.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class UserTimePair implements WritableComparable<UserTimePair> {

	private Text mdn;
	private LongWritable trackTime;

	public UserTimePair() {
		set(new Text(), new LongWritable());
	}

	public UserTimePair(String mdn, long timeStamp) {
		set(new Text(mdn), new LongWritable(timeStamp));
	}

	public UserTimePair(Text mdn, LongWritable trackTime) {
		set(mdn, trackTime);
	}

	public void set(Text mdn, LongWritable trackTime) {
		this.mdn = mdn;
		this.trackTime = trackTime;
	}

	public Text getMdn() {
		return mdn;
	}

	public LongWritable getTrackTime() {
		return trackTime;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		mdn.readFields(in);
		trackTime.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		mdn.write(out);
		trackTime.write(out);
	}

	@Override
	public int compareTo(UserTimePair o) {
		int cmp = mdn.compareTo(o.getMdn());
		if (cmp != 0) {
			return cmp;
		}
		return trackTime.compareTo(o.getTrackTime());
	}

}
