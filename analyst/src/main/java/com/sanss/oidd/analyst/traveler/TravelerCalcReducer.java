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

	private final String TransportMapFileURL = "/user/nmger/oidd/share/transport.txt";
	private final int IN = 1;
	private final int OUT = 2;

	private HashMap<String, String> transportMap;
	private String CALC_DATE;
	private String CALC_DATE_PRE;
	private String CALC_DATE_NEXT;

	private int OFFLINE_MIN_DIFFS;
	private int IN_MIN_DIFFS = 7200;
	private int OUT_MIN_DIFFS = 7200;

	private Text keyOutput = new Text();

	@Override
	protected void reduce(Text key, Iterable<EventTSArray> values,
			Context context) throws IOException, InterruptedException {
		EventTSArray preArray = new EventTSArray(), nextArray = new EventTSArray(), targetArray = new EventTSArray();
		EventInfo info = null;
		String firstTargetDate = null, lastTargetDate = null, mark_date = null, mark_bdid = null, date = null, loc = null, imsi = null;
		boolean inOK = false;
		int first_online, last_online, current = 0, last = 0, mark = 0;

		for (EventTSArray array : values) {
			if (array.get() != null && array.get().length > 0) {
				date = transTrackDate2Date(((EventInfo) array.get()[0])
						.getTrackDate().toString());
				if (date.equals(CALC_DATE)) {
					targetArray.set(array.get());
					info = (EventInfo) targetArray.get()[0];
					firstTargetDate = info.getTrackDate().toString();
					imsi = info.getImsi().toString();

					info = (EventInfo) targetArray.get()[targetArray.get().length - 1];
					lastTargetDate = info.getTrackDate().toString();
				} else if (date.equals(CALC_DATE_PRE)) {
					preArray.set(array.get());
				} else if (date.equals(CALC_DATE_NEXT)) {
					nextArray.set(array.get());
				}
			}
		}

		if (targetArray != null && targetArray.get() != null) {

			// calculate "first_online"
			first_online = findFirstOnlineTime(firstTargetDate, preArray);
			// calculate "last_online"
			last_online = findLastOnlineTime(lastTargetDate, nextArray);

			// calculate traveler
			last = last_online;
			for (Writable w : targetArray.get()) {
				info = (EventInfo) w;
				date = info.getTrackDate().toString();
				current = Common.getSeconds(date);
				loc = generateTransportMapKey(info.getCell().toString(), info
						.getSector().toString());

				// adjust "first_online"
				if (skipOffline(current, last)) {

					// judge OUT event
					if (mark != 0 && last - mark <= OUT_MIN_DIFFS) {
						keyOutput.set(formatKey(key.toString(), imsi,
								mark_date, mark_bdid, OUT));
						context.write(keyOutput, NullWritable.get());
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
									transportMap.get(loc), IN));
							context.write(keyOutput, NullWritable.get());
							inOK = true;
						}
					}
					// mark the last "loc" appeared in the transportMap
					mark = current;
					mark_date = date;
					mark_bdid = transportMap.get(loc);
				}// :if

				last = current;
			}// :for

			// judge last "mark" OUT event
			if (mark != 0 && last_online - mark <= OUT_MIN_DIFFS) {
				keyOutput.set(formatKey(key.toString(), imsi, mark_date,
						mark_bdid, OUT));
				context.write(keyOutput, NullWritable.get());
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

		CALC_DATE = context.getConfiguration().get(
				"oidd.analyst.traveler.calc.date");
		CALC_DATE_PRE = context.getConfiguration().get(
				"oidd.analyst.traveler.calc.predate");
		CALC_DATE_NEXT = context.getConfiguration().get(
				"oidd.analyst.traveler.calc.nextdate");
		OFFLINE_MIN_DIFFS = context.getConfiguration().getInt(
				"oidd.analyst.traveler.calc.offline", 21600);

		if (CALC_DATE == null || CALC_DATE_PRE == null
				|| CALC_DATE_NEXT == null) {
			throw new IOException(
					"parameters (oidd.analyst.traveler.calc.date | oidd.analyst.traveler.calc.predate | oidd.analyst.traveler.calc.nextdate) not set");
		}
	}

	private String generateTransportMapKey(String cell, String ci) {
		return new StringBuffer(cell).append("@").append(ci).toString();
	}

	private String formatKey(String mdn, String imsi, String date, String bdid,
			int inORout) {
		return new StringBuilder(mdn).append(FIELDS_SEPARATOR).append(imsi)
				.append(FIELDS_SEPARATOR).append(date).append(FIELDS_SEPARATOR)
				.append(bdid).append(FIELDS_SEPARATOR).append(inORout)
				.toString();
	}

	private String transTrackDate2Date(String trackDate) {
		return trackDate.substring(0, 11).replaceAll("\\D", "");
	}

	private boolean skipOffline(int current, int last) {
		return current - last >= OFFLINE_MIN_DIFFS;
	}

	private int findFirstOnlineTime(String firstTargetDate, EventTSArray pre) {
		int current = 0, last = 0;
		String date = null;
		EventInfo info = null;

		last = Common.getSeconds(firstTargetDate);

		if (pre != null && pre.get() != null) {
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

	private int findLastOnlineTime(String lastTargetDate, EventTSArray next) {
		int current = 0, last = 0;
		String date = null;
		EventInfo info = null;

		last = Common.getSeconds(lastTargetDate);

		if (next != null && next.get() != null) {
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
