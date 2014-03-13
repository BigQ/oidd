package com.sanss.oidd.analyst.bd;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BusinessDistrictAnalyseDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner
				.run(new BusinessDistrictAnalyseDriver(), args);
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

		Job job1 = Job.getInstance(getConf());
		job1.setJobName("BusinessDistrict Analyse -> Filter Stage");
		job1.setJarByClass(getClass());
		// set map-reduce
		job1.setMapperClass(BusinessDistrictFilterMapper.class);
		job1.setCombinerClass(IntSumReducer.class);
		job1.setReducerClass(IntSumReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1,
				new Path(args[1] + "/intermediate"));

		if (job1.waitForCompletion(true)) {
			Job job2 = Job.getInstance(getConf());
			job2.getConfiguration().set("mapreduce.job.reduces", "1");
			job2.setJobName("BusinessDistrict Analyse -> Group Stage");
			job2.setJarByClass(getClass());
			// set map-reduce
			job2.setMapperClass(BusinessDistrictGroupMapper.class);
			job2.setReducerClass(BusinessDistrictGroupReducer.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(NullWritable.class);
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
			FileInputFormat.setInputPaths(job2, new Path(args[1]
					+ "/intermediate/part*"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/result"));
			return job2.waitForCompletion(true) ? 0 : 1;
		}
		return 1;
	}

}
