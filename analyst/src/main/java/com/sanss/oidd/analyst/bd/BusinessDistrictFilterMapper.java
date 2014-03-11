package com.sanss.oidd.analyst.bd;

import static com.sanss.oidd.analyst.utils.Common.C_COUNTER_G_SKIPRECORD;
import static com.sanss.oidd.analyst.utils.Common.C_COUNTER_SKIPRECORD_ERROR;
import static com.sanss.oidd.analyst.utils.Common.getSecondsInDay;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.sanss.oidd.analyst.utils.Common;
import com.sanss.oidd.common.io.EventInfo;
import com.sanss.oidd.common.io.EventTSArray;


public class BusinessDistrictFilterMapper extends
		Mapper<Text, EventTSArray, Text, IntWritable> {

	private HashMap<String, String> businessMap;
	private final String BusinessMapFileURL = "/user/nmger/oidd/share/business.txt";
	private final String BUSINESS_MAP_COUNTER = "BusinessMapFileCounter";
	private final String BUSINESS_MAP_COUNTER_ITEMS = "Items";
	
	private IntWritable linger = new IntWritable();
	
	@Override
	protected void map(Text key, EventTSArray value, Context context)
			throws IOException, InterruptedException {
		String cell = null;
		String sector = null;
		int diffs = 0;

		int lastDiffs = 0;
		String lastArea = null;
		String lastDate = null;
		boolean lastOK = false;

		String mdn = key.toString();

		if (value.get() != null) {
			for (Writable w : value.get()) {
				EventInfo e = (EventInfo) w;
				cell = e.getCell().toString();
				sector = e.getSector().toString();
				try {
					diffs = getSecondsInDay(e.getTrackDate().toString());
				} catch (Exception ex) {
					context.getCounter(C_COUNTER_G_SKIPRECORD,
							C_COUNTER_SKIPRECORD_ERROR).increment(1);
					break;
				}

				if (lastOK) {
					linger.set(diffs - lastDiffs);
					context.write(
							new Text(generateBusinessLingerKey(lastArea,
									lastDate, mdn)), linger);
				}

				String businessMapKey = generateBusinessMapKey(cell, sector);
				if (businessMap.containsKey(businessMapKey)) {
					lastDiffs = diffs;
					lastDate = e.getTrackDate().toString();
					lastArea = businessMap.get(businessMapKey);
					lastOK = true;
				} else {
					lastOK = false;
				}
			}

			if (lastOK) {
				linger.set(Math.min(Common.C_V_SECONDSINDAY_MAX - lastDiffs,
						Common.C_V_EVENT_MAX_INTERVAL));

				context.write(
						new Text(generateBusinessLingerKey(lastArea, lastDate,
								mdn)), linger);
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		businessMap = new HashMap<>();
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path path = new Path(BusinessMapFileURL);
		StringTokenizer token;
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(
				fs.open(path)))) {

			for (String line = reader.readLine(); line != null; line = reader
					.readLine()) {
				token = new StringTokenizer(line, ",");
				if (token.countTokens() == 3) {
					String area = token.nextToken();
					businessMap.put(
							generateBusinessMapKey(token.nextToken(),
									token.nextToken()), area);
					context.getCounter(BUSINESS_MAP_COUNTER,
							BUSINESS_MAP_COUNTER_ITEMS).increment(1);
				}
			}

		}
	}

	private String generateBusinessMapKey(String cell, String ci) {
		return new StringBuffer(cell).append("@").append(ci).toString();
	}

	private String generateBusinessLingerKey(String area, String date,
			String mdn) {
		return new StringBuffer(area).append("@").append(date).append("@")
				.append(mdn).toString();
	}
}
