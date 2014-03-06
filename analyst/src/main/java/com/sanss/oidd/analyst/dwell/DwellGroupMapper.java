package com.sanss.oidd.analyst.dwell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
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
	private HashMap<String, IntWritable> switchoverGroup = new HashMap<>();
	private HashMap<String, IntWritable> noiseGroup = new HashMap<>();
	protected static final int NOISE_THRESHOLD = 3;

	@Override
	protected void map(Text key, LocStayArray value, Context context)
			throws IOException, InterruptedException {

		LocStayInfo info;
		List<Group> marks = new ArrayList<>();

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

		// find all linger items and group the switch-over items;
		while (index < array.length) {
			// merge whether a noise data appeared
			info = (LocStayInfo) array[index];
			if (index + 1 < array.length
					&& info.getSpan().get() < Common.C_V_EVENT_CYLIC_BONUS
					&& info.getNb().get() < 2) {
				// do merge
			}

			// find the switch-over group
			if (index + 3 < array.length) {
				begin = index;
				index = findSwitchoverGroupLastIndex(array, begin);
				if (index > begin) {
					// flush switch-over group
					String groupName = flushGroup(array, 1, begin, index,
							begin, index, context);
					// mark the index
					marks.add(new Group(begin, index, groupName));
					index++;
					continue;
				}
			}

			// find the linger group
			info = (LocStayInfo) array[index];
			if (info.getBegin().get() >= Common.C_V_SERVICE_PEAK_START
					&& info.getBegin().get() <= Common.C_V_SERVICE_PEAK_END
					&& info.getSpan().get() > Common.C_V_LINGER_PEAK_THRED
					|| (info.getBegin().get() < Common.C_V_SERVICE_PEAK_START || info
							.getBegin().get() > Common.C_V_SERVICE_PEAK_END)
					&& info.getSpan().get() > Common.C_V_LINGER_IDLE_THRED) {
				switchoverGroup.clear();
				switchoverGroup.put(info.getLoc().toString(), new IntWritable(
						Math.max(1, info.getNb().get())));
				// flush linger group
				String groupName = flushGroup(array, 0, index, index, index,
						index, context);
				// mark the index
				marks.add(new Group(begin, index, groupName));

				index++;
			} else {
				// skip
				index++;
			}// :end if
		}// :end while
		/**
		 * // find all passing items if (marks.size() > 0 && marks.get(0)[0] >
		 * 0) { begin = index = 0; while (begin < marks.get(0)[0]) { index =
		 * findPassGroupLastIndex(array, begin, marks.get(0)[0] - 1);
		 * flushGroup(array, 2, begin, (index == marks.get(0)[0] - 1 ? index + 1
		 * : index), begin, index, context); begin = index + 1; } } for (int i =
		 * 1; i < marks.size(); i++) { begin = index = marks.get(i - 1)[1] + 1;
		 * while (begin < marks.get(i)[0]) { index =
		 * findPassGroupLastIndex(array, begin, marks.get(i)[0] - 1);
		 * flushGroup(array, 2, (begin == marks.get(i - 1)[1] + 1 ? begin - 1 :
		 * begin), (index == marks.get(i)[0] - 1 ? index + 1 : index), begin,
		 * index, context); begin = index + 1; } } if (marks.size() > 0 &&
		 * marks.get(marks.size() - 1)[1] < array.length - 1) { begin = index =
		 * marks.get(marks.size() - 1)[1] + 1; while (begin < array.length) {
		 * index = findPassGroupLastIndex(array, begin, array.length - 1);
		 * flushGroup( array, 2, (begin == marks.get(marks.size() - 1)[1] + 1 ?
		 * begin - 1 : begin), index, begin, index, context); begin = index + 1;
		 * } }
		 */
	}

	private int findSwitchoverGroupLastIndex(Writable[] array, int start) {
		
		int mark = start;
		int last = start + 1;
		String loc = null;
		int repeats = 0;
		IntWritable count;
		int noise = 0;

		switchoverGroup.clear();
		noiseGroup.clear();
		// put the first item to the group
		loc = ((LocStayInfo) array[start]).getLoc().toString();
		switchoverGroup.put(loc, new IntWritable(1));

		while (last < array.length) {
			loc = ((LocStayInfo) array[last]).getLoc().toString();
			repeats = ((LocStayInfo) array[last]).getNb().get();
			if (switchoverGroup.containsKey(loc)) {
				count = switchoverGroup.get(loc);
				count.set(count.get() + Math.max(1, repeats));
				noise = Math.max(noise - Math.max(1, repeats), 0);
				if (noise < 1) {
					mark = last;
				}
			} else if (noiseGroup.containsKey(loc) && noise < NOISE_THRESHOLD) {
				switchoverGroup.put(loc, new IntWritable(noiseGroup.get(loc)
						.get() + 1));
				noiseGroup.remove(loc);
			} else if (noise < NOISE_THRESHOLD) {
				if (repeats > 1) {
					switchoverGroup.put(loc, new IntWritable(repeats));
				} else {
					noiseGroup.put(loc, new IntWritable(1));
					noise = noise + Math.max(1, repeats * 2);
				}
			} else {
				break;
			}

			last++;
		}

		loc = ((LocStayInfo) array[start]).getLoc().toString();
		if (switchoverGroup.get(loc).get() > 1
				&& noiseGroup.size() < switchoverGroup.size()) {
			return mark;
		} else {
			return start;
		}
	}

	private String flushGroup(Writable[] array, int type, int begin, int end,
			int groupFrom, int groupTo, Context context) throws IOException,
			InterruptedException {
		mapOutputValue.getType().set(type);

		LocStayInfo info = (LocStayInfo) array[groupFrom];
		mapOutputValue.getDate().set(info.getDate().copyBytes());
		mapOutputValue.getBegin().set(info.getBegin().get());
		mapOutputValue.getEnd().set(info.getBegin().get());
		String groupName = getSwitchoverGroupName(switchoverGroup);
		mapOutputValue.getSource().set(groupName);

		/**
		 * // set location info if (type == 2) { info = (LocStayInfo)
		 * array[begin];
		 * mapOutputValue.getSource().set(info.getLoc().copyBytes()); info =
		 * (LocStayInfo) array[end];
		 * mapOutputValue.getLoc2().set(info.getLoc().copyBytes()); } else if
		 * (type == 0) { info = (LocStayInfo) array[groupFrom];
		 * mapOutputValue.getLoc1().set(info.getLoc().copyBytes()); } else {
		 * SortedMap<String, IntWritable> sortedMap = new TreeMap<>(
		 * switchoverGroup); StringBuilder sb = new StringBuilder(); for (String
		 * loc : sortedMap.keySet()) { sb.append("|").append(loc); }
		 * mapOutputValue.getLoc1().set(sb.substring(1)); }
		 */
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
		return groupName;
	}

	private String getSwitchoverGroupName(Map<String, IntWritable> group) {
		SortedMap<String, IntWritable> sortedMap = new TreeMap<>(
				switchoverGroup);
		StringBuilder sb = new StringBuilder();
		for (String loc : sortedMap.keySet()) {
			sb.append("|").append(loc);
		}
		return sb.substring(1);
	}

	private int findPassGroupLastIndex(Writable[] array, int start, int end) {
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

	class Group {
		int begin;
		int end;
		String name;

		public Group(int begin, int end, String name) {
			this.begin = begin;
			this.end = end;
			this.name = name;
		}
	}
}
