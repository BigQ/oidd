package com.sanss.oidd.analyst.bd;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusinessDistrictGroupMapper extends
		Mapper<Text, IntWritable, Text, IntWritable> {

	private String area;
	private String mdn;

	@Override
	protected void map(Text key, IntWritable value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer token = new StringTokenizer(key.toString(), "@");
		if (token.countTokens() == 3) {
			area = token.nextToken();
			token.nextToken();
			mdn = token.nextToken();
			context.write(new Text(generateMapperKey(area, mdn)), value);
		}
	}

	private String generateMapperKey(String area, String mdn) {
		return area + "@" + mdn;
	}

}
