package org.gqmsite.oidd.analyst.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class EventInfo implements Writable {

	private final Text mdn;
	private final Text trackDate; // track date
	private final IntWritable diffs; // seconds
	private final IntWritable event;
	private final Text cell;
	private final IntWritable sector;

	public EventInfo() {
		this.mdn = new Text();
		this.trackDate = new Text();
		this.diffs = new IntWritable();
		this.event = new IntWritable();
		this.cell = new Text();
		this.sector = new IntWritable();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		mdn.readFields(in);
		trackDate.readFields(in);
		diffs.readFields(in);
		event.readFields(in);
		cell.readFields(in);
		sector.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		mdn.write(out);
		trackDate.write(out);
		diffs.write(out);
		event.write(out);
		cell.write(out);
		sector.write(out);
	}

	public Text getMdn() {
		return mdn;
	}

	public Text getTrackDate() {
		return trackDate;
	}

	public IntWritable getDiffs() {
		return diffs;
	}

	public IntWritable getEvent() {
		return event;
	}

	public Text getCell() {
		return cell;
	}

	public IntWritable getSector() {
		return sector;
	}

	@Override
	public String toString() {
		return new StringBuilder().append(mdn.toString()).append(",")
				.append(trackDate.toString()).append(",")
				.append(diffs.toString()).append(",").append(event.toString())
				.append(",").append(cell.toString()).append(",")
				.append(sector.toString()).toString();
	}

	public EventInfo copy() {
		EventInfo info = new EventInfo();
		info.getMdn().set(mdn.copyBytes());
		info.getTrackDate().set(trackDate.copyBytes());
		info.getDiffs().set(diffs.get());
		info.getEvent().set(event.get());
		info.getCell().set(cell.copyBytes());
		info.getSector().set(sector.get());
		return info;
	}
}
