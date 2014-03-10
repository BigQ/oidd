package com.sanss.oidd.analyst.dwell;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sanss.oidd.common.io.DwellActivityItem;

public class DwellFilterMapper extends
		Mapper<Text, DwellActivityItem, Text, DwellActivityItem> {

	protected static final String FILTER_HH_LIST = "oidd.dwell.filter.hh.list";

	private String filterHHList;
	private Text mapOutputKey = new Text();
	private DwellActivityItem mapOutputValue = new DwellActivityItem();

	@Override
	protected void map(Text key, DwellActivityItem value, Context context)
			throws IOException, InterruptedException {
		if (filterHHList.contains(value.getHh().toString())) {
			mapOutputKey.set(key.toString() + "," + value.getLoc().toString());
			mapOutputValue.getDate().set(value.getDate().copyBytes());
			mapOutputValue.getAc().set(value.getAc().get());
			context.write(mapOutputKey, value);
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		filterHHList = context.getConfiguration().get(FILTER_HH_LIST);
	}

}
