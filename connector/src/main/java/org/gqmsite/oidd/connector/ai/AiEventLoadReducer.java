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

	private Event datum = new Event();

	@Override
	protected void reduce(Text key, Iterable<EventInfo> values, Context context)
			throws IOException, InterruptedException {
		for (EventInfo info : values) {
			datum.setImsi(info.getImsi().toString());
			datum.setMdn(info.getMdn().toString());
			datum.setTrackDate(info.getTrackDate().toString());
			datum.setEvent(info.getEvent().get());
			datum.setCell(info.getCell().toString());
			datum.setSector(info.getSector().toString());
			datum.setPeer(info.getPeer().toString());

			context.write(new AvroKey<>(datum), NullWritable.get());
		}
	}

}
