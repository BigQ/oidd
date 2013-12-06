package org.gqmsite.oidd.analyst.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class LingerPair implements WritableComparable<LingerPair> {

	private Text mdn;
	private Text cell;
	private IntWritable sector;
	private Text trackDate;
	private IntWritable type;// 0:night, 1:day

	public LingerPair() {
		mdn = new Text();
		cell = new Text();
		sector = new IntWritable();
		trackDate = new Text();
		type = new IntWritable();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		mdn.readFields(in);
		cell.readFields(in);
		sector.readFields(in);
		trackDate.readFields(in);
		type.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		mdn.write(out);
		cell.write(out);
		sector.write(out);
		trackDate.write(out);
		type.write(out);
	}

	public Text getMdn() {
		return mdn;
	}

	public Text getCell() {
		return cell;
	}

	public IntWritable getSector() {
		return sector;
	}

	public Text getTrackDate() {
		return trackDate;
	}

	public IntWritable getType() {
		return type;
	}

	@Override
	public int compareTo(LingerPair target) {
		int cmp = mdn.compareTo(target.getMdn());
		if (cmp != 0) {
			return cmp;
		}
		cmp = type.compareTo(target.getType());
		if (cmp != 0) {
			return cmp;
		}
		cmp = cell.compareTo(target.getCell());
		if (cmp != 0) {
			return cmp;
		}
		cmp = sector.compareTo(target.getSector());
		if (cmp != 0) {
			return cmp;
		}
		cmp = trackDate.compareTo(target.getTrackDate());
		if (cmp != 0) {
			return cmp;
		}

		return 0;
	}

	@Override
	public String toString() {
		return new StringBuilder().append(mdn.toString()).append(":")
				.append(trackDate.toString()).append(":").append(type.get())
				.append(":").append(cell.toString()).append(":")
				.append(sector.get()).toString();
	}

}
