package org.gqmsite.oidd.analyst.extract.ts;

import org.apache.hadoop.mapreduce.Partitioner;
import org.gqmsite.oidd.analyst.io.EventInfo;
import org.gqmsite.oidd.analyst.io.UserTimePair;

public class UserPartitioner extends Partitioner<UserTimePair, EventInfo> {

	@Override
	public int getPartition(UserTimePair key, EventInfo value,
			int numPartitions) {
		return key.getMdn().toString().hashCode() % numPartitions;
	}

}
