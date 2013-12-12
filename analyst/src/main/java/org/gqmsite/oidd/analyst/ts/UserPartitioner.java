package org.gqmsite.oidd.analyst.ts;

import org.apache.hadoop.mapreduce.Partitioner;
import org.gqmsite.oidd.analyst.io.EventInfo;
import org.gqmsite.oidd.analyst.io.UserTimePair;

public class UserPartitioner extends Partitioner<UserTimePair, EventInfo> {

	@Override
	public int getPartition(UserTimePair key, EventInfo value, int numPartitions) {
		return Math.abs(key.getKey().toString().substring(9).hashCode()) % numPartitions;
	}

}
