package org.gqmsite.oidd.analyst.linger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.gqmsite.oidd.analyst.io.LingerPair;

public class LingerPairPartitioner extends Partitioner<LingerPair, IntWritable> {

	@Override
	public int getPartition(LingerPair key, IntWritable value, int numPartitions) {
		return Math.abs(key.getMdn().hashCode()) % numPartitions;
	}

}
