package com.sanss.oidd.common.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DwellActivityItem implements Writable {

	private final Text loc;
	private final Text date;
	private final Text hh;
	private final IntWritable ac;

	public DwellActivityItem() {
		this.loc = new Text();
		this.date = new Text();
		this.hh = new Text();
		this.ac = new IntWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		loc.write(out);
		date.write(out);
		hh.write(out);
		ac.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		loc.readFields(in);
		date.readFields(in);
		hh.readFields(in);
		ac.readFields(in);
	}

	@Override
	public String toString() {
		return new StringBuilder().append(loc.toString())
				.append(loc.toString().length() > 7 ? "\t" : "\t\t")
				.append(date.toString()).append('\t').append(hh.toString())
				.append('\t').append(ac.get()).toString();
	}

	public Text getLoc() {
		return loc;
	}

	public Text getDate() {
		return date;
	}

	public Text getHh() {
		return hh;
	}

	public IntWritable getAc() {
		return ac;
	}

}
