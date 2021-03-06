package com.sanss.oidd.common.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DwellGroup implements Writable {

	private final Text date;
	/**
	 * the group type, 0: linger, 1:switch-over , 2: location shift
	 */
	private final IntWritable type;
	private final IntWritable begin;
	private final IntWritable end;
	private final Text source;
	private final Text target;
	private final DwellItemArray group;

	public DwellGroup() {
		this.date = new Text();
		this.type = new IntWritable();
		this.begin = new IntWritable();
		this.end = new IntWritable();
		this.source = new Text();
		this.target = new Text();
		this.group = new DwellItemArray();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		date.write(out);
		type.write(out);
		group.write(out);
		begin.write(out);
		end.write(out);
		source.write(out);
		target.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		date.readFields(in);
		type.readFields(in);
		group.readFields(in);
		begin.readFields(in);
		end.readFields(in);
		source.readFields(in);
		target.readFields(in);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("{\n\ttm:")
				.append(date.toString()).append(", type:").append(type.get())
				.append(", locs:").append(getLocs()).append(", st:")
				.append(begin.get()).append(", ed:").append(end.get())
				.append(", span:").append(getSpan()).append(", [\n");
		for (String s : group.toStrings()) {
			sb.append("\t\t").append(s).append("\n");
		}
		sb.append("\t]\n}");
		return sb.toString();
	}

	public String getLocs() {
		if (type.get() == 2) {
			return source.toString() + "-" + target.toString();
		} else {
			return source.toString();
		}
	}

	public int getSpan() {
		return end.get() - begin.get();
	}

	public Text getDate() {
		return date;
	}

	public IntWritable getType() {
		return type;
	}

	public IntWritable getBegin() {
		return begin;
	}

	public IntWritable getEnd() {
		return end;
	}

	public DwellItemArray getGroup() {
		return group;
	}

	public Text getSource() {
		return source;
	}

	public Text getTarget() {
		return target;
	}
}
