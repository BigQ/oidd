package org.gqmsite.oidd.analyst.utils;

import org.apache.hadoop.conf.Configured;
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

		Path map = new Path(mapUri);
		try (MapFile.Reader reader = new MapFile.Reader(map, getConf())) {
			Text mdn = new Text(args[1]);
			// new instance
			Writable value = (Writable) reader.getValueClass().newInstance();
			// read value
			reader.get(mdn, value);
			// print value
			System.out.println(value.toString());
		}
		return 0;
	}

}
