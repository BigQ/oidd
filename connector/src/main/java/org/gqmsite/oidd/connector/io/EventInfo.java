package org.gqmsite.oidd.connector.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class EventInfo implements Writable {

	private final Text mdn;
	private final Text imsi;
	private final Text trackDate; // track date
	private final Text peer;
	private final IntWritable event;
	private final Text cell;
	private final IntWritable sector;

	public EventInfo() {
		mdn = new Text();
		imsi = new Text();
		trackDate = new Text();
		peer = new Text();
		event = new IntWritable();
		cell = new Text();
		sector = new IntWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		mdn.write(out);
		imsi.write(out);
		trackDate.write(out);
		peer.write(out);
		event.write(out);
		cell.write(out);
		sector.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		mdn.readFields(in);
		imsi.readFields(in);
		trackDate.readFields(in);
		peer.readFields(in);
		event.readFields(in);
		cell.readFields(in);
		sector.readFields(in);
	}

	public Text getMdn() {
		return mdn;
	}

	public Text getImsi() {
		return imsi;
	}

	public Text getTrackDate() {
		return trackDate;
	}

	public Text getPeer() {
		return peer;
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
