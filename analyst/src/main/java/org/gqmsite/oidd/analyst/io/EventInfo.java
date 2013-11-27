package org.gqmsite.oidd.analyst.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class EventInfo implements Writable {

	private final Text mdn;
	private final LongWritable trackTime;
	private final IntWritable event;
	private final Text cell;
	private final IntWritable sector;

	public EventInfo() {
		this.mdn = new Text();
		this.trackTime = new LongWritable();
		this.event = new IntWritable();
		this.cell = new Text();
		this.sector = new IntWritable();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		mdn.readFields(in);
		trackTime.readFields(in);
		event.readFields(in);
		cell.readFields(in);
		sector.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		mdn.write(out);
		trackTime.write(out);
		event.write(out);
		cell.write(out);
		sector.write(out);
	}

	public Text getMdn() {
		return mdn;
	}

	public LongWritable getTrackTime() {
		return trackTime;
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
				.append(trackTime.toString()).append(",")
				.append(event.toString()).append(",").append(cell.toString())
				.append(",").append(sector.toString()).toString();
	}

	public EventInfo copy() {
		EventInfo info = new EventInfo();
		info.getMdn().set(mdn.copyBytes());
		info.getTrackTime().set(trackTime.get());
		info.getEvent().set(event.get());
		info.getCell().set(cell.copyBytes());
		info.getSector().set(sector.get());
		return info;
	}
}
