package org.gqmsite.oidd.analyst.cleansing;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.gqmsite.oidd.analyst.io.EventInfo;

public class EventInfoCleansingMapper extends
		Mapper<LongWritable, Text, Text, EventInfo> {

	private EventInfo info = new EventInfo();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Calendar calendar = Calendar.getInstance();

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
			} else if (index == 6) { // parse trackDate and diff-seconds
				try {
					info.getTrackDate().set(temp.substring(0,11).replaceAll("\\D", ""));
					calendar.setTime(sdf.parse(temp));
					info.getDiffs()
							.set(calendar.get(Calendar.SECOND)
									+ calendar.get(Calendar.MINUTE) * 60
									+ calendar.get(Calendar.HOUR_OF_DAY) * 3600);
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
				context.write(info.getMdn(), info);
			}

			index++;
		}
	}

}
