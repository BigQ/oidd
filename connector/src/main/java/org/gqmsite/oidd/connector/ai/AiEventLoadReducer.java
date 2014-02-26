package org.gqmsite.oidd.connector.ai;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.gqmsite.oidd.connector.io.EventInfo;
import org.gqmsite.oidd.connector.schema.Event;

public class AiEventLoadReducer extends
		Reducer<Text, EventInfo, AvroKey<Event>, NullWritable> {

	private AvroKey<Event> outputKey = new AvroKey<>();

	@Override
	protected void reduce(Text key, Iterable<EventInfo> values, Context context)
			throws IOException, InterruptedException {
		for (EventInfo info : values) {
			outputKey.datum(convert(info));
			context.write(outputKey, NullWritable.get());
		}
	}

	private Event convert(EventInfo info) {
		return Event.newBuilder().setImsi(info.getImsi().toString())
				.setMdn(info.getMdn().toString())
				.setTrackDate(info.getTrackDate().toString())
				.setEvent(info.getEvent().get())
				.setPeer(info.getPeer().toString())
				.setCell(info.getCell().toString())
				.setSector(info.getSector().toString()).build();
	}
}
