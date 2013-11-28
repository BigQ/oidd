package org.gqmsite.oidd.analyst.cleansing;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.gqmsite.oidd.analyst.io.EventInfo;

public class EventInfoCleansingPartitioner extends Partitioner<Text, EventInfo> {

	@Override
	public int getPartition(Text key, EventInfo value, int numPartitions) {
		return (value.getDiffs().get() / 3600) % numPartitions;
	}

}
