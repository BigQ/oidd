package com.sanss.oidd.analyst.traveler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.sanss.oidd.analyst.utils.Common;
import com.sanss.oidd.common.io.EventInfo;
import com.sanss.oidd.common.io.EventTSArray;

public class TravelerCalcReducer extends
		Reducer<Text, EventTSArray, Text, NullWritable> {

	protected static final String FIELDS_SEPARATOR = ",";

	private HashMap<String, String> transportMap;
	private final String TransportMapFileURL = "/user/nmger/oidd/share/transport.txt";
	private final int IN = 1;
	private final int OUT = 2;

	private String calc_date;
	private String calc_predate;
	private String calc_nextdate;
	private int OFFLINE_MIN_DIFFS;
	private int IN_MIN_DIFFS = 7200;
	private int OUT_MIN_DIFFS = 7200;

	private Text keyOutput = new Text();

	@Override
	protected void reduce(Text key, Iterable<EventTSArray> values,
			Context context) throws IOException, InterruptedException {
		EventTSArray target = null;
		EventTSArray pre = null;
		EventTSArray next = null;
		EventInfo info;
		String date = null, mark_date = null, loc = null, imsi = null;
		boolean inOK = false;
		int first_online, last_online, current = 0, last = 0, mark = 0;

		for (EventTSArray array : values) {
			date = transTrackDate2Date(((EventInfo) array.get()[0])
					.getTrackDate().toString());
			if (date.equals(calc_date)) {
				target = array;
				imsi = ((EventInfo) array.get()[0]).getImsi().toString();
			} else if (date.equals(calc_predate)) {
				pre = array;
			} else if (date.equals(calc_nextdate)) {
				next = array;
			}
		}

		if (target != null) {
			// "first_online"
			first_online = findFirstOnlineTime(target, pre);
			// "last_online"
			last_online = findLastOnlineTime(target, next);

			last = last_online;
			for (Writable w : target.get()) {
				info = (EventInfo) w;
				date = info.getTrackDate().toString();
				current = Common.getSeconds(date);
				loc = generateTransportMapKey(info.getCell().toString(), info
						.getSector().toString());

				// adjust "first_online"
				if (skipOffline(current, last)) {

					// judge OUT event
					if (mark != 0 && current - mark <= OUT_MIN_DIFFS) {
						keyOutput.set(formatKey(key.toString(), imsi,
								mark_date, OUT));
						mark = 0;
					}
					// update the "first_online"
					first_online = current;
					inOK = false;
				}// :if

				// judge whether the "loc" appeared in the transportMap
				if (transportMap.containsKey(loc)) {
					if (!inOK) {
						// judge IN event
						if (current - first_online <= IN_MIN_DIFFS) {
							keyOutput.set(formatKey(key.toString(), imsi, date,
									IN));
							context.write(keyOutput, NullWritable.get());
							inOK = true;
						}
					}
					// mark the last "loc" appeared in the transportMap
					mark = current;
					mark_date = date;
				}// :if

				last = current;
			}// :for

			// judge last "mark" OUT event
			if (mark != 0 && last_online - mark <= OUT_MIN_DIFFS) {
				keyOutput.set(formatKey(key.toString(), imsi, mark_date, OUT));
			}
		}// :if
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		transportMap = new HashMap<>();
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path path = new Path(TransportMapFileURL);
		StringTokenizer token;
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(
				fs.open(path)))) {

			for (String line = reader.readLine(); line != null; line = reader
					.readLine()) {
				token = new StringTokenizer(line, ",");
				if (token.countTokens() == 3) {
					String area = token.nextToken();
					transportMap.put(
							generateTransportMapKey(token.nextToken(),
									token.nextToken()), area);
				}
			}

		}

		calc_date = context.getConfiguration().get(
				"oidd.analyst.traveler.calc.date");
		calc_predate = context.getConfiguration().get(
				"oidd.analyst.traveler.calc.predate");
		calc_nextdate = context.getConfiguration().get(
				"oidd.analyst.traveler.calc.nextdate");
		OFFLINE_MIN_DIFFS = context.getConfiguration().getInt(
				"oidd.analyst.traveler.calc.offline", 21600);

		if (calc_date == null || calc_predate == null || calc_nextdate == null) {
			throw new IOException(
					"parameters (oidd.analyst.traveler.calc.date | oidd.analyst.traveler.calc.predate | oidd.analyst.traveler.calc.nextdate) not set");
		}
	}

	private String generateTransportMapKey(String cell, String ci) {
		return new StringBuffer(cell).append("@").append(ci).toString();
	}

	private String formatKey(String mdn, String imsi, String date, int inORout) {
		return new StringBuilder(mdn).append(FIELDS_SEPARATOR).append(imsi)
				.append(FIELDS_SEPARATOR).append(date).append(FIELDS_SEPARATOR)
				.append(inORout).toString();
	}

	private String transTrackDate2Date(String trackDate) {
		return trackDate.substring(0, 11).replaceAll("\\D", "");
	}

	private boolean skipOffline(int current, int last) {
		return current - last >= OFFLINE_MIN_DIFFS;
	}

	private int findFirstOnlineTime(EventTSArray target, EventTSArray pre) {
		int current = 0, last = 0;
		String date = null;
		EventInfo info = null;

		// the calc_day first event
		date = transTrackDate2Date(((EventInfo) target.get()[0]).getTrackDate()
				.toString());
		last = Common.getSeconds(date);

		if (pre != null) {
			for (int i = pre.get().length - 1; i >= 0; i--) {
				info = (EventInfo) pre.get()[i];
				date = info.getTrackDate().toString();
				current = Common.getSeconds(date);
				// adjust "first_online", find the last SkipOffline
				if (skipOffline(last, current)) {
					break;
				}
				last = current;
			}
		}

		return last;
	}

	private int findLastOnlineTime(EventTSArray target, EventTSArray next) {
		int current = 0, last = 0;
		String date = null;
		EventInfo info = null;

		date = transTrackDate2Date(((EventInfo) target.get()[target.get().length - 1])
				.getTrackDate().toString());
		last = Common.getSeconds(date);

		if (next != null) {
			for (Writable w : next.get()) {
				info = (EventInfo) w;
				date = info.getTrackDate().toString();
				current = Common.getSeconds(date);
				// adjust "last_online", find the first SkipOffline
				if (skipOffline(current, last)) {
					break;
				}
				last = current;
			}

		}

		return last;
	}
}
