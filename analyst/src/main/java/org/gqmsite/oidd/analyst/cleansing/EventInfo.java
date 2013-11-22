package org.gqmsite.oidd.analyst.cleansing;

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

}
