package com.sanss.oidd.analyst.dwell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.sanss.oidd.analyst.utils.Common;
import com.sanss.oidd.common.io.DwellGroup;
import com.sanss.oidd.common.io.DwellItem;
import com.sanss.oidd.common.io.LocStayArray;
import com.sanss.oidd.common.io.LocStayInfo;

public class DwellGroupMapper extends
		Mapper<Text, LocStayArray, Text, DwellGroup> {

	private DwellGroup mapOutputValue = new DwellGroup();
	private List<DwellItem> container = new ArrayList<>();

	@Override
	protected void map(Text key, LocStayArray value, Context context)
			throws IOException, InterruptedException {

		LocStayInfo info1, info2, info3, info4;
		DwellItem item = null;

		for (Writable w : value.get()) {
			item = new DwellItem();
			copyLocStay2DwellItem(item, (LocStayInfo) w);
			container.add(item);
		}

		Writable[] array = value.get();
		int index = 0;
		int begin = 0;
		while (index < array.length) {

			// find the ABAB... pattern
			if (index < array.length - 3) {
				info1 = (LocStayInfo) array[index];
				info2 = (LocStayInfo) array[index + 1];
				info3 = (LocStayInfo) array[index + 2];
				info4 = (LocStayInfo) array[index + 3];
				if (info1.getLoc().toString().equals(info3.getLoc().toString())
						&& info2.getLoc().toString()
								.equals(info4.getLoc().toString())
						&& info1.getBegin().get() == (info2.getBegin().get() + info2
								.getSpan().get())
						&& info2.getBegin().get() == (info3.getBegin().get() + info3
								.getSpan().get())
						&& info3.getBegin().get() == (info4.getBegin().get() + info4
								.getSpan().get())) {
					// mark the start item
					begin = index;
					// do merge
					mergeDwellItem(container.get(index),
							container.get(index + 2));

					mergeDwellItem(container.get(index + 1),
							container.get(index + 3));

					index = index + 4;
					while (index < array.length) {
						info1 = (LocStayInfo) array[index];
						info2 = (LocStayInfo) array[index - 1];
						info3 = (LocStayInfo) array[index - 2];

						if (info1.getLoc().toString()
								.equals(info3.getLoc().toString())
								&& info1.getBegin().get() == (info2.getBegin()
										.get() + info2.getSpan().get())
								&& info2.getBegin().get() == (info3.getBegin()
										.get() + info3.getSpan().get())) {
							// do merge
							mergeDwellItem(container.get(index - 2),
									container.get(index));

							index++;
						} else {
							break;
						}

					}

					info1 = (LocStayInfo) array[begin];
					mapOutputValue.getBegin().set(info1.getBegin().get());
					mapOutputValue.getDate().set(info1.getDate().copyBytes());
					mapOutputValue.getEnd().set(info1.getBegin().get());
					mapOutputValue.getType().set(0);// dwell

					Writable[] arr = new Writable[2];
					item = container.get(index - 2);
					arr[0] = item;
					mapOutputValue.getEnd().set(
							mapOutputValue.getEnd().get()
									+ item.getSpan().get());

					item = container.get(index - 1);
					arr[1] = item;
					mapOutputValue.getEnd().set(
							mapOutputValue.getEnd().get()
									+ item.getSpan().get());
					mapOutputValue.getGroup().set(arr);
					context.write(key, mapOutputValue);
					continue;
				}
			}

			info1 = (LocStayInfo) array[index];
			if (info1.getSpan().get() < Common.C_V_EVENT_CYCLE_FULL) {
				// mark the start item
				begin = index;
				index++;
				// group the short lingering items
				while (index < array.length) {
					info1 = (LocStayInfo) array[index];
					info2 = (LocStayInfo) array[index - 1];
					if (info1.getBegin().get() == (info2.getBegin().get() + info2
							.getSpan().get())
							&& info1.getSpan().get() < Common.C_V_EVENT_CYCLE_FULL) {
						index++;
					} else {
						break;
					}
				}

				info1 = (LocStayInfo) array[begin];
				mapOutputValue.getBegin().set(info1.getBegin().get());
				mapOutputValue.getDate().set(info1.getDate().copyBytes());
				mapOutputValue.getEnd().set(info1.getBegin().get());
				mapOutputValue.getType().set(1);// short-dwell

				Writable[] arr = new Writable[index - begin];
				for (int i = begin; i < index; i++) {
					item = container.get(i);
					arr[i-begin] = item;
					mapOutputValue.getEnd().set(
							mapOutputValue.getEnd().get()
									+ item.getSpan().get());
				}
				mapOutputValue.getGroup().set(arr);
				context.write(key, mapOutputValue);
			} else {
				mapOutputValue.getBegin().set(info1.getBegin().get());
				mapOutputValue.getDate().set(info1.getDate().copyBytes());
				mapOutputValue.getEnd().set(info1.getBegin().get());
				mapOutputValue.getType().set(0);// dwell
				Writable[] arr = new Writable[1];
				item = container.get(index);
				arr[0] = item;
				mapOutputValue.getEnd().set(
						mapOutputValue.getEnd().get() + item.getSpan().get());
				mapOutputValue.getGroup().set(arr);
				context.write(key, mapOutputValue);
				index++;
			}
		}
	}

	private void copyLocStay2DwellItem(final DwellItem item,
			final LocStayInfo info) {
		item.getLoc().set(info.getLoc().copyBytes());
		item.getSpan().set(info.getSpan().get());
		item.getC0().set(info.getC0().get());
		item.getC1().set(info.getC1().get());
		item.getM0().set(info.getM0().get());
		item.getM1().set(info.getM1().get());
		item.getX1().set(info.getX1().get());
	}

	private void mergeDwellItem(final DwellItem source, final DwellItem target) {
		target.getSpan().set(target.getSpan().get() + source.getSpan().get());
		target.getC0().set(target.getC0().get() + source.getC0().get());
		target.getC1().set(target.getC1().get() + source.getC1().get());
		target.getM0().set(target.getM0().get() + source.getM0().get());
		target.getM1().set(target.getM1().get() + source.getM1().get());
		target.getX1().set(target.getX1().get() + source.getX1().get());
	}
}
