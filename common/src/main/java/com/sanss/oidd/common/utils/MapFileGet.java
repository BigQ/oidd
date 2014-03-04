package com.sanss.oidd.common.utils;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapFileGet extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MapFileGet(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage %s [generic options] <file> <mdn>\n",
					getClass().getName());
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String mapUri = args[0];
		FileSystem fs = FileSystem.get(getConf());
		Path path = new Path(mapUri);
		FileStatus[] status = fs.globStatus(path);
		Path[] listPaths = FileUtil.stat2Paths(status);
		for (Path map : listPaths) {
			try (MapFile.Reader reader = new MapFile.Reader(map, getConf())) {
				Text mdn = new Text(args[1]);
				// new instance
				Writable value = (Writable) reader.getValueClass()
						.newInstance();
				// read value
				if (reader.get(mdn, value) != null) {
					// print value
					System.out.println(value.toString());
					while (reader.next(mdn, value)) {
						if (!mdn.toString().endsWith(args[1])) {
							break;
						}
						System.out.println(value.toString());
					}
				}

			}
		}
		return 0;
	}

}
