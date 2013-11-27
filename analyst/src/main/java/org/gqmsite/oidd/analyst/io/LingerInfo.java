package org.gqmsite.oidd.analyst.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class LingerInfo implements Writable {

	private final Text startTime;
	private final LongWritable lingering;
	private final Text cell;
	private final IntWritable sector;

	public LingerInfo() {
		startTime = new Text();
		lingering = new LongWritable();
		cell = new Text();
		sector = new IntWritable();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		cell.readFields(in);
		sector.readFields(in);
		startTime.readFields(in);
		lingering.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		cell.write(out);
		sector.write(out);
		startTime.write(out);
		lingering.write(out);
	}

	public Text getStartTime() {
		return startTime;
	}

	public LongWritable getLingering() {
		return lingering;
	}

	public Text getCell() {
		return cell;
	}

	public IntWritable getSector() {
		return sector;
	}

}
