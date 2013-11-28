package org.gqmsite.oidd.analyst.extract.ts;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.gqmsite.oidd.analyst.io.EventArray;
import org.gqmsite.oidd.analyst.io.EventInfo;
import org.gqmsite.oidd.analyst.io.UserTimePair;

public class UserEventTSDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UserEventTSDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage %s [generic options] <in> <out>\n",
					getClass().getName());
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Job job = Job.getInstance(getConf());
		job.setJobName("User Event Time Series Formatter");
		job.setJarByClass(getClass());

		job.setMapperClass(UserEventTSMapper.class);
		job.setPartitionerClass(UserPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setReducerClass(UserEventTSReducer.class);

		job.setMapOutputKeyClass(UserTimePair.class);
		job.setMapOutputValueClass(EventInfo.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(EventArray.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, MapFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class KeyComparator extends WritableComparator {

		protected KeyComparator() {
			super(UserTimePair.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			UserTimePair p1 = (UserTimePair) a;
			UserTimePair p2 = (UserTimePair) b;
			int cmp = p1.getMdn().compareTo(p2.getMdn());
			if (cmp != 0) {
				return cmp;
			}
			return p1.getTrackTime().compareTo(p2.getTrackTime());
		}

	}

	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(UserTimePair.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			UserTimePair p1 = (UserTimePair) a;
			UserTimePair p2 = (UserTimePair) b;
			return p1.getMdn().compareTo(p2.getMdn());
		}
	}
}
