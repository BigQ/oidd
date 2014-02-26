package org.gqmsite.oidd.connector.ai;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.gqmsite.oidd.connector.schema.Event;

public class AiEventLoadMapper extends
		Mapper<LongWritable, Text, AvroKey<Event>, NullWritable> {

	private AiEventParser parser = new AiEventParser();
	private Event record = new Event();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		parser.parse(value.toString());

		if (parser.isParsed()) {
			record.setImsi(parser.getImsi());
			record.setMdn(parser.getMdn());
			record.setTrackDate(parser.getTrackDate());
			record.setEvent(parser.getEvent());
			record.setCell(parser.getCell());
			record.setSector(parser.getSector());
			record.setPeer(parser.getPeer());

			context.write(new AvroKey<>(record), NullWritable.get());
		}
	}

}
