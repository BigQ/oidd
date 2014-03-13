package com.sanss.oidd.analyst.dwell;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sanss.oidd.common.io.DwellActivityItem;
import com.sanss.oidd.common.io.DwellGroup;
import com.sanss.oidd.common.io.EventTSArray;
import com.sanss.oidd.common.io.LocStayArray;

public class UserDwellCalcDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UserDwellCalcDriver(), args);
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
		job.setJobName("User Location Stay Calculate and Group and Analyse");
		job.setJarByClass(getClass());

		// set map-reduce
		ChainMapper.addMapper(job, LocStayCalcMapper.class, Text.class,
				EventTSArray.class, Text.class, LocStayArray.class, getConf());
		ChainMapper.addMapper(job, DwellGroupMapper.class, Text.class,
				LocStayArray.class, Text.class, DwellGroup.class, getConf());
		ChainMapper.addMapper(job, UserDwellFormatMapper.class, Text.class,
				DwellGroup.class, Text.class, DwellActivityItem.class,
				getConf());

		// set output key and value type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// set input output
		job.setInputFormatClass(SequenceFileInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
