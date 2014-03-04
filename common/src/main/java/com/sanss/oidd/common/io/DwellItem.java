package com.sanss.oidd.common.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DwellItem implements Writable {

	private final Text loc;

	/**
	 * measure unit, calculate the dwellTime
	 */
	private final IntWritable span;
	/**
	 * measure unit, sum-up the count of the calling event
	 */
	private final IntWritable c0;
	/**
	 * measure unit, sum-up the count of the called
	 */
	private final IntWritable c1;
	/**
	 * measure unit, sum-up the count of the short message
	 */
	private final IntWritable m0;
	/**
	 * measure unit, sum-up the count of the short message sent
	 */
	private final IntWritable m1;
	/**
	 * measure unit, sum-up the count of the short message received
	 */
	private final IntWritable x1;

	public DwellItem() {
		this.loc = new Text();

		this.span = new IntWritable();
		this.c0 = new IntWritable();
		this.c1 = new IntWritable();
		this.m0 = new IntWritable();
		this.m1 = new IntWritable();
		this.x1 = new IntWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		loc.write(out);

		span.write(out);
		c0.write(out);
		c1.write(out);
		m0.write(out);
		m1.write(out);
		x1.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		loc.readFields(in);

		span.readFields(in);
		c0.readFields(in);
		c1.readFields(in);
		m0.readFields(in);
		m1.readFields(in);
		x1.readFields(in);
	}

	@Override
	public String toString() {
		return new StringBuilder("{loc:").append(loc.toString())
				.append(", span:").append(span.get()).append(", c0:")
				.append(c0.get()).append(", c1:").append(c1.get())
				.append(", m0:").append(m0.get()).append(", m1:")
				.append(m1.get()).append(", x1:").append(x1.get()).append("}")
				.toString();
	}

	public Text getLoc() {
		return loc;
	}

	public IntWritable getSpan() {
		return span;
	}

	public IntWritable getC0() {
		return c0;
	}

	public IntWritable getC1() {
		return c1;
	}

	public IntWritable getM0() {
		return m0;
	}

	public IntWritable getM1() {
		return m1;
	}

	public IntWritable getX1() {
		return x1;
	}

}
