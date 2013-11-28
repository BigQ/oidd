package org.gqmsite.oidd.analyst.cleansing;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.gqmsite.oidd.analyst.io.EventInfo;

public class EventInfoCleansingPartitioner extends Partitioner<Text, EventInfo> {

	@Override
	public int getPartition(Text key, EventInfo value, int numPartitions) {
		String hour = key.toString().substring(9, 11);
		return Integer.parseInt(hour) % numPartitions;
	}

}
