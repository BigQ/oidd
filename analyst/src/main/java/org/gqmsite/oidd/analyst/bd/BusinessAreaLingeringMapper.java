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
import org.apache.hadoop.mapreduce.Mapper;
import org.gqmsite.oidd.analyst.io.LingerInfo;

public class BusinessAreaLingeringMapper extends
		Mapper<Text, LingerInfo, Text, IntWritable> {

	private HashMap<String, String> businessMap;
	private final String BusinessMapFileURL = "/user/nmger/oidd/share/business.txt";

	@Override
	protected void map(Text key, LingerInfo value, Context context)
			throws IOException, InterruptedException {
		String businessMapKey = generateBusinessMapKey(value.getCell()
				.toString(), value.getSector().toString());
		if (businessMap.containsKey(businessMapKey)) {
			context.write(
					new Text(generateBusinessLingerKey(businessMap
							.get(businessMapKey), value.getTrackDate()
							.toString(), key.toString())), value.getLingering());
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
