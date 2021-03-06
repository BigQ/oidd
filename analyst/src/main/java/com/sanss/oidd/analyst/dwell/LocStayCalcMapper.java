package com.sanss.oidd.analyst.dwell;

import static com.sanss.oidd.analyst.utils.Common.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.sanss.oidd.common.io.EventInfo;
import com.sanss.oidd.common.io.EventTSArray;
import com.sanss.oidd.common.io.LocStayArray;
import com.sanss.oidd.common.io.LocStayInfo;

public class LocStayCalcMapper extends
		Mapper<Text, EventTSArray, Text, LocStayArray> {

	// private Text outputKey = new Text();
	private LocStayArray outputValue = new LocStayArray();
	private List<LocStayInfo> container = new ArrayList<>();

	@Override
	protected void map(Text key, EventTSArray value, Context context)
			throws IOException, InterruptedException {

		EventInfo eventInfo = null;
		LocStayInfo stay = null;
		String loc = null;
		int diffs = 0;
		int event = -1;
		String lastLoc = null;
		int lastDiffs = 0;

		for (Writable w : value.get()) {
			eventInfo = (EventInfo) w;
			loc = getLoc(eventInfo.getCell().toString(), eventInfo.getSector()
					.get());
			event = eventInfo.getEvent().get();
			try {
				diffs = getSecondsInDay(eventInfo.getTrackDate().toString());
			} catch (Exception ex) {
				context.getCounter(C_COUNTER_G_SKIPRECORD,
						C_COUNTER_SKIPRECORD_ERROR).increment(1);
				break;
			}

			if (lastLoc != null && loc.equals(lastLoc)
					&& diffs - lastDiffs <= C_V_EVENT_MAX_INTERVAL) {
				// do merge when it is the same stay
				// merge the span time of the stay
				stay.getSpan().set(stay.getSpan().get() + (diffs - lastDiffs));
				// count the event in each stay
				countEventInStay(event, stay);
			} else {
				// adjust the current diffs
				if (lastLoc == null) {
					// adjust when the first event in a day
					diffs = Math.max(diffs - C_V_EVENT_CYLIC_BONUS, 0);
				} else if (event == C_EVENT_CYLICLOCATIONUPDATE
						&& !loc.equals(lastLoc)) {
					// adjust when the first event in a stay is
					// CYLICLOCATIONUPDATE
					if (diffs - lastDiffs > C_V_EVENT_MIN_INTERVAL) {
						diffs = Math.max(diffs - C_V_EVENT_CYLIC_BONUS,
								lastDiffs + C_V_EVENT_MIN_INTERVAL);
					}
				}
				// add the last stay to the container
				if (stay != null) {
					// update the span time of the stay
					stay.getSpan().set(
							stay.getSpan().get()
									+ Math.min(diffs - lastDiffs,
											C_V_EVENT_MAX_INTERVAL));
					// add the last stay to the container
					container.add(stay);
				}

				// create a new stay
				stay = new LocStayInfo();
				stay.getLoc().set(loc);
				stay.getDate().set(
						eventInfo.getTrackDate().toString().substring(0, 11)
								.replaceAll("\\D", ""));
				stay.getBegin().set(diffs);
				// count the event in each stay
				countEventInStay(event, stay);
			}
			// update the last Location
			lastLoc = loc;
			// update the last diffs
			lastDiffs = diffs;
		}

		if (stay != null) {
			// adjust the diffs to the last stay in a day
			diffs = Math.min(diffs + C_V_EVENT_MAX_INTERVAL,
					C_V_SECONDSINDAY_MAX);

			stay.getSpan().set(stay.getSpan().get() + diffs - lastDiffs);
			// add the last stay to the container
			container.add(stay);
		}

		// check whether it is the illegal MDN
		if (eventInfo.getMdn().toString().length() != C_V_ILLEGAL_MDN_LEN) {
			context.getCounter(C_COUNTER_G_SKIPRECORD,
					C_COUNTER_SKIPRECORD_ILLMDN).increment(1);
		} else if (container.size() > 0) {
			// flush the container items
			Writable[] arr = new Writable[container.size()];
			outputValue.set(container.toArray(arr));
			context.write(key, outputValue);
		}
		container.clear();
	}

	private void countEventInStay(int event, LocStayInfo stay) {
		switch (event) {
		case C_EVENT_CALLING:
			stay.getC0().set(stay.getC0().get() + 1);
			break;
		case C_EVENT_CALLED:
			stay.getC1().set(stay.getC1().get() + 1);
			break;
		case C_EVENT_SMSSEND:
			stay.getM0().set(stay.getM0().get() + 1);
			break;
		case C_EVENT_SMSRECEIVE:
			stay.getM1().set(stay.getM1().get() + 1);
			break;
		case C_EVENT_X1ONLINE:
			stay.getX1().set(stay.getX1().get() + 1);
			break;
		default:
			stay.getNb().set(stay.getNb().get() + 1);
			break;
		}
	}

}
