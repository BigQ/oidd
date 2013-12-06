package org.gqmsite.oidd.analyst.linger;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.gqmsite.oidd.analyst.io.LingerPair;
import org.gqmsite.oidd.analyst.io.LocationMeasure;

public class LocationStaySumupReducer extends
		Reducer<LingerPair, IntWritable, Text, LocationMeasure> {

	private Text outKey = new Text();
	private LocationMeasure outValue = new LocationMeasure();
	private HashSet<String> measures = new HashSet<>();
	private String lastMdn = null;
	private String lastCell = null;
	private int lastSector = 0;
	private int lastType = 0;
	private int lastStays = 0;

	@Override
	protected void reduce(LingerPair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		String mdn = key.getMdn().toString();
		String cell = key.getCell().toString();
		String trackDate = key.getTrackDate().toString();
		int sector = key.getSector().get();
		int type = key.getType().get();

		int summary = 0;
		for (IntWritable value : values) {
			summary += value.get();
		}

		if (lastMdn != null && mdn.equals(lastMdn) && cell.equals(lastCell)
				&& sector == lastSector && type == lastType) {
			measures.add(trackDate);
			lastStays += summary;
		} else {
			if (lastMdn != null) {
				outKey.set(lastMdn);
				outValue.getCell().set(lastCell);
				outValue.getSector().set(lastSector);
				outValue.getType().set(lastType);
				outValue.getDays().set(measures.size());
				outValue.getStays().set(lastStays);
				context.write(outKey, outValue);
			}

			lastMdn = mdn;
			lastCell = cell;
			lastSector = sector;
			lastType = type;
			lastStays = summary;
			measures.clear();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		if (lastMdn != null) {
			outKey.set(lastMdn);
			outValue.getCell().set(lastCell);
			outValue.getSector().set(lastSector);
			outValue.getType().set(lastType);
			outValue.getDays().set(measures.size());
			outValue.getStays().set(lastStays);
			context.write(outKey, outValue);
		}
	}

}
