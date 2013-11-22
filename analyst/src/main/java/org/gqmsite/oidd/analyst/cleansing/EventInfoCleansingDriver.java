package org.gqmsite.oidd.analyst.cleansing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EventInfoCleansingDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new EventInfoCleansingDriver(), args);
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

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("Event Information Cleansing");
		job.setJarByClass(EventInfoCleansingDriver.class);
		job.setMapperClass(EventInfoCleansingMapper.class);
		// job.setPartitionerClass(EventInfoCleansingPartitioner.class);
		job.setReducerClass(EventInfoCleansingReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(EventInfo.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(EventInfo.class);
		LazyOutputFormat.setOutputFormatClass(job,
				SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// job.setNumReduceTasks(2);

		return job.waitForCompletion(true) ? 0 : 1;

	}

}
