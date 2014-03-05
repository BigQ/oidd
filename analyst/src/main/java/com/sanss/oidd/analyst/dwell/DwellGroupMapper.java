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
	private Text mapOutputKey;
	private List<DwellItem> container = new ArrayList<>();

	@Override
	protected void map(Text key, LocStayArray value, Context context)
			throws IOException, InterruptedException {

		LocStayInfo info1, info2, info3, info4;
		List<int[]> marks = new ArrayList<>();

		int index = 0;
		int begin = 0;

		Writable[] array = value.get();
		mapOutputKey = key;
		// initial the container
		for (int i = 0; i < array.length; i++) {
			if (container.size() < i + 1) {
				container.add(new DwellItem());
			}
			copyLocStay2DwellItem(container.get(i), (LocStayInfo) array[i]);
		}

		// find all dwell items and group the ABAB... items;
		while (index < array.length) {
			// find the ABAB... pattern
			if (index + 3 < array.length) {
				info1 = (LocStayInfo) array[index];
				info2 = (LocStayInfo) array[index + 1];
				info3 = (LocStayInfo) array[index + 2];
				info4 = (LocStayInfo) array[index + 3];
				if (info1.getLoc().toString().equals(info3.getLoc().toString())
						&& info2.getLoc().toString()
								.equals(info4.getLoc().toString())
						&& info2.getBegin().get() == (info1.getBegin().get() + info1
								.getSpan().get())
						&& info3.getBegin().get() == (info2.getBegin().get() + info2
								.getSpan().get())
						&& info4.getBegin().get() == (info3.getBegin().get() + info3
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
					// flush ABAB... group
					flushGroup(array, 1, begin, begin + 1, index - 2,
							index - 1, context);
					// mark the index
					int[] mark = { begin, index - 1 };
					marks.add(mark);
					continue;
				}// :end if, check the ABAB pattern
			}// :end if, check enough items for ABAB pattern

			info1 = (LocStayInfo) array[index];
			if (info1.getSpan().get() >= Common.C_V_EVENT_CYCLE_FULL) {
				// flush linger group
				flushGroup(array, 0, index, index, index, index, context);
				// mark the index
				int[] mark = { index, index };
				marks.add(mark);

				index++;
			} else {
				// skip
				index++;
			}// :end if
		}// :end while

		// find all passing items
		if (marks.size() > 0 && marks.get(0)[0] > 0) {
			begin = index = 0;
			while (begin < marks.get(0)[0]) {
				index = findLastIndex(array, begin, marks.get(0)[0] - 1);
				flushGroup(array, 2, begin,
						(index == marks.get(0)[0] - 1 ? index + 1 : index),
						begin, index, context);
				begin = index + 1;
			}
		}
		for (int i = 1; i < marks.size(); i++) {
			begin = index = marks.get(i - 1)[1] + 1;
			while (begin < marks.get(i)[0]) {
				index = findLastIndex(array, begin, marks.get(i)[0] - 1);
				flushGroup(array, 2,
						(begin == marks.get(i - 1)[1] + 1 ? begin - 1 : begin),
						(index == marks.get(i)[0] - 1 ? index + 1 : index),
						begin, index, context);
				begin = index + 1;
			}
		}
		if (marks.size() > 0
				&& marks.get(marks.size() - 1)[1] < array.length - 1) {
			begin = index = marks.get(marks.size() - 1)[1] + 1;
			while (begin < array.length) {
				index = findLastIndex(array, begin, array.length - 1);
				flushGroup(
						array,
						2,
						(begin == marks.get(marks.size() - 1)[1] + 1 ? begin - 1
								: begin), index, begin, index, context);
				begin = index + 1;
			}
		}
	}

	private void flushGroup(Writable[] array, int type, int begin, int end,
			int groupFrom, int groupTo, Context context) throws IOException,
			InterruptedException {
		LocStayInfo info = (LocStayInfo) array[begin];
		mapOutputValue.getDate().set(info.getDate().copyBytes());
		mapOutputValue.getType().set(type);
		mapOutputValue.getLoc1().set(info.getLoc().copyBytes());
		info = (LocStayInfo) array[end];
		mapOutputValue.getLoc2().set(info.getLoc().copyBytes());

		// set the begin
		if (type != 1) {
			info = (LocStayInfo) array[groupFrom];
			mapOutputValue.getBegin().set(info.getBegin().get());
			mapOutputValue.getEnd().set(info.getBegin().get());
		} else {
			info = (LocStayInfo) array[begin];
			mapOutputValue.getBegin().set(info.getBegin().get());
			mapOutputValue.getEnd().set(info.getBegin().get());
		}

		// set the group & calculate the end
		Writable[] arr = new Writable[groupTo - groupFrom + 1];
		DwellItem item = null;
		for (int i = groupFrom; i <= groupTo; i++) {
			item = container.get(i);
			arr[i - groupFrom] = item;
			mapOutputValue.getEnd().set(
					mapOutputValue.getEnd().get() + item.getSpan().get());
		}
		mapOutputValue.getGroup().set(arr);
		context.write(mapOutputKey, mapOutputValue);
	}

	private int findLastIndex(Writable[] array, int start, int end) {
		LocStayInfo info1, info2;
		int last = start + 1;
		info1 = (LocStayInfo) array[start];
		while (last <= end) {
			info1 = (LocStayInfo) array[last];
			info2 = (LocStayInfo) array[last - 1];
			if (info1.getBegin().get() == (info2.getBegin().get() + info2
					.getSpan().get())) {
				last++;
			} else {
				break;
			}
		}
		return last - 1;
	}

	private void copyLocStay2DwellItem(final DwellItem item,
			final LocStayInfo info) {
		item.getLoc().set(info.getLoc().toString());
		item.getSpan().set(info.getSpan().get());
		item.getC0().set(info.getC0().get());
		item.getC1().set(info.getC1().get());
		item.getM0().set(info.getM0().get());
		item.getM1().set(info.getM1().get());
		item.getX1().set(info.getX1().get());
		item.getNb().set(info.getNb().get());
	}

	private void mergeDwellItem(final DwellItem source, final DwellItem target) {
		target.getSpan().set(target.getSpan().get() + source.getSpan().get());
		target.getC0().set(target.getC0().get() + source.getC0().get());
		target.getC1().set(target.getC1().get() + source.getC1().get());
		target.getM0().set(target.getM0().get() + source.getM0().get());
		target.getM1().set(target.getM1().get() + source.getM1().get());
		target.getX1().set(target.getX1().get() + source.getX1().get());
		target.getNb().set(target.getNb().get() + source.getNb().get());
	}
}
