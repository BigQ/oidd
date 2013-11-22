package org.gqmsite.oidd.analyst.cleansing;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EventInfoCleansingMapper extends
		Mapper<LongWritable, Text, Text, EventInfo> {

	private EventInfo info = new EventInfo();
	private Text trackTime = new Text();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString(), ",");
		String temp;
		int index = 0;

		while (itr.hasMoreElements()) {
			temp = itr.nextToken();

			if (index == 0) { // parse cell
				info.getCell().set(temp);
			} else if (index == 1) { // parse sector
				if (StringUtils.isNumeric(temp)) {
					info.getSector().set(Integer.parseInt(temp));
				} else {
					// maybe add a counter to count the invalid elements
					break;
				}
			} else if (index == 4) { // parse MDN
				if (temp.length() >= 4) {
					info.getMdn().set(temp);
				} else {
					// maybe add a counter to count the invalid elements
					break;
				}
			} else if (index == 6) { // parse trackTime
				try {
					info.getTrackTime().set(sdf.parse(temp).getTime());
					trackTime.set(temp);
				} catch (ParseException ex) {
					// maybe add a counter to count the invalid elements
					break;
				}
			} else if (index == 7) { // parse event type
				if (StringUtils.isNumeric(temp)) {
					info.getEvent().set(Integer.parseInt(temp));
				} else {
					// maybe add a counter to count the invalid elements
					break;
				}
				context.write(trackTime, info);
			}

			index++;
		}
	}

}
