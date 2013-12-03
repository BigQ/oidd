package org.gqmsite.oidd.analyst.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class LingerInfo implements Writable {

	private final Text trackDate;
	private final IntWritable start;
	private final IntWritable end;
	private final IntWritable lingering;
	private final Text cell;
	private final IntWritable sector;

	public LingerInfo() {
		trackDate = new Text();
		start = new IntWritable();
		end = new IntWritable();
		lingering = new IntWritable();
		cell = new Text();
		sector = new IntWritable();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		cell.readFields(in);
		sector.readFields(in);
		trackDate.readFields(in);
		start.readFields(in);
		end.readFields(in);
		lingering.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		cell.write(out);
		sector.write(out);
		trackDate.write(out);
		start.write(out);
		end.write(out);
		lingering.write(out);
	}

	public Text getTrackDate() {
		return trackDate;
	}

	public IntWritable getStart() {
		return start;
	}

	public IntWritable getEnd() {
		return end;
	}

	public IntWritable getLingering() {
		return lingering;
	}

	public Text getCell() {
		return cell;
	}

	public IntWritable getSector() {
		return sector;
	}

}
