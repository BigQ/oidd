package org.gqmsite.oidd.analyst.bd;

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
import org.gqmsite.oidd.analyst.io.EventArray;
import org.gqmsite.oidd.analyst.io.EventInfo;
import org.gqmsite.oidd.analyst.utils.Common;

public class BusinessAreaEventMapper extends
		Mapper<Text, EventArray, Text, IntWritable> {

	private HashMap<String, String> businessMap;
	private final String BusinessMapFileURL = "/user/nmger/oidd/share/business.txt";
	private IntWritable linger = new IntWritable();

	@Override
	protected void map(Text key, EventArray value, Context context)
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
				diffs = e.getDiffs().get();

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
				linger.set(Math.min(Common.MAX_DIFFS - lastDiffs,
						Common.CYLIC_BONUS));

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
			token = new StringTokenizer(reader.readLine(), ",");
			if (token.countTokens() == 3) {
				String area = token.nextToken();
				businessMap.put(
						generateBusinessMapKey(token.nextToken(),
								token.nextToken()), area);
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
