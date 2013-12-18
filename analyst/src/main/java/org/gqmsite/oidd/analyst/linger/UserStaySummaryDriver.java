package org.gqmsite.oidd.analyst.linger;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.gqmsite.oidd.analyst.io.EventArray;
import org.gqmsite.oidd.analyst.io.LingerInfo;
import org.gqmsite.oidd.analyst.io.LingerPair;
import org.gqmsite.oidd.analyst.io.LocationMeasure;

public class UserStaySummaryDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UserStaySummaryDriver(), args);
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
		job.setJobName("User Location Estimate Chain");
		job.setJarByClass(getClass());

		// extract linger event and merge the information to LingerInfo
		ChainMapper.addMapper(job, UserLingeringMapper.class, Text.class,
				EventArray.class, Text.class, LingerInfo.class,
				job.getConfiguration());
		// calculate the stay with the LingerInfo
		ChainMapper.addMapper(job, LingerCalculateMapper.class, Text.class,
				LingerInfo.class, LingerPair.class, IntWritable.class,
				job.getConfiguration());

		// sum up the stay
		ChainReducer.setReducer(job, LocationStaySumupReducer.class,
				LingerPair.class, IntWritable.class, Text.class,
				LocationMeasure.class, job.getConfiguration());

		// set job configurations
		job.setInputFormatClass(SequenceFileInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, MapFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setCombinerClass(LingerCalculateReducer.class);
		job.setPartitionerClass(LingerPairPartitioner.class);
		job.setMapOutputKeyClass(LingerPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LocationMeasure.class);

		// set compress option
		SequenceFileOutputFormat.setOutputCompressionType(job,
				CompressionType.BLOCK);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
