package org.gqmsite.oidd.analyst.linger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.gqmsite.oidd.analyst.io.LingerInfo;
import org.gqmsite.oidd.analyst.io.LingerPair;
import org.gqmsite.oidd.analyst.utils.Common;

public class LingerCalculateMapper extends
		Mapper<Text, LingerInfo, LingerPair, IntWritable> {

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	private LingerPair pair = new LingerPair();
	private IntWritable period = new IntWritable();

	@Override
	protected void map(Text key, LingerInfo value, Context context)
			throws IOException, InterruptedException {
		int stay = 0;

		stay = Common.calculateCurrentNightLinger(value.getStart().get(), value
				.getEnd().get());
		if (stay > 0) {
			pair.getMdn().set(key.toString());
			pair.getCell().set(value.getCell().toString());
			pair.getSector().set(value.getSector().get());
			pair.getTrackDate().set(value.getTrackDate().toString());
			pair.getType().set(0);
			period.set(stay);
			context.write(pair, period);
		}

		stay = Common.calculateCurrentDayLinger(value.getStart().get(), value
				.getEnd().get());
		if (stay > 0) {
			pair.getMdn().set(key.toString());
			pair.getCell().set(value.getCell().toString());
			pair.getSector().set(value.getSector().get());
			pair.getTrackDate().set(value.getTrackDate().toString());
			pair.getType().set(1);
			period.set(stay);
			context.write(pair, period);
		}

		stay = Common.calculateNextNightLinger(value.getStart().get(), value
				.getEnd().get());
		if (stay > 0) {
			try {
				pair.getMdn().set(key.toString());
				pair.getCell().set(value.getCell().toString());
				pair.getSector().set(value.getSector().get());
				// calculate the day after the trackDate
				long time = sdf.parse(value.getTrackDate().toString())
						.getTime() + 24 * 3600 * 1000;
				pair.getTrackDate().set(sdf.format(new Date(time)));
				pair.getType().set(0);
				period.set(stay);
				context.write(pair, period);
			} catch (ParseException ex) {
				// :TODO add a counter if it need
			}
		}
	}

}
