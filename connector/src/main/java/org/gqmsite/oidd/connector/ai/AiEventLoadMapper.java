package org.gqmsite.oidd.connector.ai;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sanss.oidd.common.io.EventInfo;

public class AiEventLoadMapper extends
		Mapper<LongWritable, Text, Text, EventInfo> {

	private AiEventParser parser = new AiEventParser();
	private EventInfo record = new EventInfo();
	private Text mapOutputKey = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		parser.parse(value.toString());

		if (parser.isParsed()) {
			record.getImsi().set(parser.getImsi());
			record.getMdn().set(parser.getMdn());
			record.getTrackDate().set(parser.getTrackDate());
			record.getEvent().set(parser.getEvent());
			record.getCell().set(parser.getCell());
			record.getSector().set(parser.getSector());
			record.getPeer().set(parser.getPeer());

			mapOutputKey.set(parser.getMdn());
			context.write(mapOutputKey, record);
		}
	}

}
