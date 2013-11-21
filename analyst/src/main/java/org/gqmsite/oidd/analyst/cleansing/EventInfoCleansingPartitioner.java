package org.gqmsite.oidd.analyst.cleansing;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class EventInfoCleansingPartitioner extends Partitioner<Text, EventInfo> {

	@Override
	public int getPartition(Text key, EventInfo value, int numPartitions) {
		String hour = value.getTrackTime().toString().substring(11, 13);
		return Integer.parseInt(hour) % numPartitions;
	}

}
