package com.sanss.oidd.common.io;

import org.apache.hadoop.io.ArrayWritable;

public class DwellItemArray extends ArrayWritable {

	public DwellItemArray() {
		super(DwellItem.class);
	}

}
