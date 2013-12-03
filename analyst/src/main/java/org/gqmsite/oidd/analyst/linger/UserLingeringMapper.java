package org.gqmsite.oidd.analyst.linger;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.gqmsite.oidd.analyst.io.EventArray;
import org.gqmsite.oidd.analyst.io.EventInfo;
import org.gqmsite.oidd.analyst.io.LingerInfo;
import org.gqmsite.oidd.analyst.utils.Common;

public class UserLingeringMapper extends
		Mapper<Text, EventArray, Text, LingerInfo> {

	private LingerInfo info = new LingerInfo();

	@Override
	protected void map(Text key, EventArray value, Context context)
			throws IOException, InterruptedException {
		String cell = null;
		int sector = 0;
		int diffs = 0;
		int event = -1;
		String lastCell = null;
		int lastSector = 0;
		int lastDiffs = 0;

		if (value.get() != null) {
			for (Writable w : value.get()) {
				EventInfo e = (EventInfo) w;
				event = e.getEvent().get();
				cell = e.getCell().toString();
				sector = e.getSector().get();
				diffs = e.getDiffs().get();
				// collect the event if the event is
				// CYLIC_LOCATION_UPDATE
				if (event == Common.EVENT_CYLIC_LOCATION_UPDATE) {
					// first collected event in day
					if (lastCell == null) {
						lastCell = cell;
						lastSector = sector;
						lastDiffs = diffs;
						// mark the linger
						info.getCell().set(cell);
						info.getSector().set(sector);
						info.getTrackDate().set(e.getTrackDate().copyBytes());
						info.getStart().set(
								Math.max(0, diffs - Common.CYLIC_FULL_BONUS));
						info.getEnd().set(diffs);
					} else {
						// merge the linger
						if (cell.equals(lastCell)
								&& sector == lastSector
								&& (diffs - lastDiffs) < Common.CYLIC_FULL_BONUS) {
							info.getEnd().set(diffs);
						} else {
							info.getLingering()
									.set(info.getEnd().get()
											- info.getStart().get());
							context.write(key, info);
							// mark the linger
							info.getCell().set(cell);
							info.getSector().set(sector);
							info.getTrackDate().set(
									e.getTrackDate().copyBytes());
							info.getStart().set(
									Math.max(lastDiffs, diffs
											- Common.CYLIC_BONUS));
							info.getEnd().set(diffs);
						}

						lastCell = cell;
						lastSector = sector;
						lastDiffs = diffs;
					}
				} else {
					// Non-CYLIC_LOCATION_UPDATE event, if the next event
					// location is the same as the Last linger location within
					// CYLIC_BONUS, update
					// the time
					if (cell.equals(lastCell) && sector == lastSector
							&& (diffs - lastDiffs) < Common.CYLIC_BONUS) {
						lastDiffs = diffs;
						info.getEnd().set(diffs);
					}
				}

			}

			if (lastCell != null) {
				// add the last bonus
				if (Common.MAX_DIFFS - lastDiffs < Common.CYLIC_BONUS) {
					info.getEnd().set(Common.MAX_DIFFS);
				}

				info.getLingering().set(
						info.getEnd().get() - info.getStart().get());
				context.write(key, info);
			}
		}
	}

}
