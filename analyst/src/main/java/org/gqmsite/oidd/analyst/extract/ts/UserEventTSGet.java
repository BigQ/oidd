package org.gqmsite.oidd.analyst.extract.ts;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.gqmsite.oidd.analyst.io.EventArray;
import org.gqmsite.oidd.analyst.io.EventInfo;

public class UserEventTSGet extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UserEventTSGet(), args);
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
		Text mdn = new Text(args[1]);
		EventArray value = new EventArray();
		Configuration conf = new Configuration();
		Path map = new Path(mapUri);
		try (MapFile.Reader reader = new MapFile.Reader(map, conf)) {
			reader.get(mdn, value);
			for (Writable item : value.get()) {
				EventInfo info = (EventInfo) item;
				System.out.println(info.toString());
			}
		}
		return 0;
	}

}
