package com.sanss.oidd.analyst.ts;

import org.apache.hadoop.mapreduce.Partitioner;

import com.sanss.oidd.common.io.EventInfo;
import com.sanss.oidd.common.io.UserTimePair;

public class EventTSPartitioner extends Partitioner<UserTimePair, EventInfo> {

	@Override
	public int getPartition(UserTimePair key, EventInfo value, int numPartitions) {
		return Math.abs(key.getUser().toString().hashCode()) % numPartitions;
	}

}
