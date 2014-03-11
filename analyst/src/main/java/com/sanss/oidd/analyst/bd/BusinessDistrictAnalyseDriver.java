package com.sanss.oidd.analyst.bd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
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

		JobControl jobChain = new JobControl(
				"BusinessDistrict Analyse, Filter & Group");
		ControlledJob job1 = new ControlledJob(getConf());
		job1.setJobName("BusinessDistrict Analyse -> Filter Stage");
		job1.getJob().setJarByClass(getClass());
		// set map-reduce
		job1.getJob().setMapperClass(BusinessDistrictFilterMapper.class);
		job1.getJob().setCombinerClass(IntSumReducer.class);
		job1.getJob().setReducerClass(IntSumReducer.class);
		job1.getJob().setOutputKeyClass(Text.class);
		job1.getJob().setOutputValueClass(IntWritable.class);
		job1.getJob().setInputFormatClass(SequenceFileInputFormat.class);
		job1.getJob().setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(job1.getJob(), new Path(args[0]));
		FileOutputFormat.setOutputPath(job1.getJob(),
				new Path(args[1] + "/intermediate"));

		ControlledJob job2 = new ControlledJob(new Configuration());
		job2.getJob().getConfiguration().set("mapreduce.job.reduces", "1");
		job2.getJob().setJobName("BusinessDistrict Analyse -> Group Stage");
		job2.getJob().setJarByClass(getClass());
		// set map-reduce
		job2.getJob().setMapperClass(BusinessDistrictGroupMapper.class);
		job2.getJob().setReducerClass(BusinessDistrictGroupReducer.class);
		job2.getJob().setMapOutputKeyClass(Text.class);
		job2.getJob().setMapOutputValueClass(IntWritable.class);
		job2.getJob().setOutputKeyClass(Text.class);
		job2.getJob().setOutputValueClass(NullWritable.class);
		job2.getJob().setInputFormatClass(SequenceFileInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job2.getJob(), TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2.getJob(), new Path(args[1]
				+ "/intermediate/part*"));
		FileOutputFormat.setOutputPath(job2.getJob(), new Path(args[1] + "/result"));
		
		// add jobs
		job2.addDependingJob(job1);
		jobChain.addJob(job1);
		jobChain.addJob(job2);
		
		jobChain.run();
		return 0;
	}

}
