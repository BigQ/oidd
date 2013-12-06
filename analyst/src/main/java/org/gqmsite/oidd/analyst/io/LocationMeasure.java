package org.gqmsite.oidd.analyst.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class LocationMeasure implements Writable {

	private Text cell;
	private IntWritable sector;
	private IntWritable type;
	private IntWritable days;
	private IntWritable stays;

	public LocationMeasure() {
		cell = new Text();
		sector = new IntWritable();
		type = new IntWritable();
		days = new IntWritable();
		stays = new IntWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		cell.write(out);
		sector.write(out);
		type.write(out);
		days.write(out);
		stays.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		cell.readFields(in);
		sector.readFields(in);
		type.readFields(in);
		days.readFields(in);
		stays.readFields(in);
	}

	public Text getCell() {
		return cell;
	}

	public IntWritable getSector() {
		return sector;
	}

	public IntWritable getType() {
		return type;
	}

	public IntWritable getDays() {
		return days;
	}

	public IntWritable getStays() {
		return stays;
	}

	@Override
	public String toString() {
		return new StringBuilder().append(type.get()).append(":")
				.append(cell.toString()).append(":").append(sector.get())
				.append(":").append(days.get()).append(":").append(stays.get())
				.toString();
	}

}
