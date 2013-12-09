package org.gqmsite.oidd.analyst.rs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.gqmsite.oidd.analyst.io.LocationMeasure;

public class ResidentEstimate extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ResidentEstimate(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.out
					.printf("Usage %s [generic options] <file> <mdn> <type> <maxDays>\n",
							getClass().getName());
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}
		String mapUri = args[0];
		type = Integer.parseInt(args[2]);
		maxDays = Integer.parseInt(args[3]);
		Path map = new Path(mapUri);
		try (MapFile.Reader reader = new MapFile.Reader(map, getConf())) {
			Text mdn = new Text(args[1]);
			// new instance
			Writable value = (Writable) reader.getValueClass().newInstance();
			// read value
			if (reader.get(mdn, value) != null) {
				// first value
				checkAndAddItem(value);
				while (reader.next(mdn, value)) {
					if (!mdn.toString().endsWith(args[1])) {
						break;
					}
					checkAndAddItem(value);
				}

				List<ResidentMeasure> list = new ArrayList<>();
				list.addAll(cells.values());
				Collections.sort(list);
				float summary = 0;
				for (ResidentMeasure item : list) {
					summary += item.getMeasure();
				}
				for(ResidentMeasure item : list){
					System.out.printf("%s, %.2f\t", item.getCell(), item.getMeasure()/summary);
				}
				System.out.println();
			}

		}
		return 0;
	}

	protected HashMap<String, ResidentMeasure> cells = new HashMap<>();
	private int type = 0;
	private int maxDays = 5;

	protected void checkAndAddItem(Writable value) {
		LocationMeasure m = (LocationMeasure) value;
		if (m.getType().get() == type) {
			String cell = m.getCell().toString();
			if (cells.containsKey(cell)) {
				cells.get(cell).updateMeasure(m.getStays().get(),
						m.getDays().get());
			} else {
				cells.put(cell, new ResidentMeasure(cell, m.getStays().get(), m
						.getDays().get()));
			}
		}
	}

	class ResidentMeasure implements Comparable<ResidentMeasure> {
		private final String cell;
		private int stay;
		private int days;

		public ResidentMeasure(String cell, int stay, int days) {
			this.cell = cell;
			this.stay = stay;
			this.days = days;
		}

		public String getCell() {
			return cell;
		}

		public int getStay() {
			return stay;
		}

		public int getDays() {
			return days;
		}

		public void updateMeasure(int stay, int days) {
			this.stay += stay;
			this.days = Math.max(this.days, days);
		}

		public float getMeasure() {
			return Math.scalb(this.stay, days - Math.max(maxDays, days));
		}

		@Override
		public int compareTo(ResidentMeasure o) {
			float diff = -(getMeasure() - o.getMeasure());
			if (diff > 0) {
				return 1;
			} else if (diff == 0) {
				return 0;
			} else {
				return -1;
			}
		}
	}
}
