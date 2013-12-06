package org.gqmsite.oidd.analyst.utils;

public class Common {

	public static final int EVENT_POWER_ON = 1;
	public static final int EVENT_POWER_OFF = 2;
	public static final int EVENT_CYLIC_LOCATION_UPDATE = 3;
	public static final int EVENT_LOCATION_UPDATE = 4;
	public static final int EVENT_SHIFT = 5;
	public static final int EVENT_CALLING = 6;
	public static final int EVENT_CALLED = 7;
	public static final int EVENT_SMS_SEND = 31;
	public static final int EVENT_SMS_RECEIVE = 32;

	public static final int CYLIC_BONUS = 1800; // seconds
	public static final int CYLIC_FULL_BONUS = 3600; // 1 hour
	public static final int MAX_DIFFS = 86399;

	public static final int NIGHT_LINGERING_PERIOD_END = 21600;
	public static final int NIGHT_LINGERING_PERIOD_START = 72000;
	public static final int DAY_LINGERING_PERIOD_START = 36000;
	public static final int DAY_LINGERING_PERIOD_END = 57600;

	/**
	 * 计算 0:00~6:00 以及 20:00~0:00之间的停留时间。单位秒
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	public static int calculateNightLingeringPeriod(int start, int end) {
		int period = 0;
		if (start >= NIGHT_LINGERING_PERIOD_END
				&& end <= NIGHT_LINGERING_PERIOD_START) {
			return period;
		}

		if (start < NIGHT_LINGERING_PERIOD_END) {
			period += (Math.min(NIGHT_LINGERING_PERIOD_END, end) - start);
		}

		if (end > NIGHT_LINGERING_PERIOD_START) {
			period += (end - Math.max(start, DAY_LINGERING_PERIOD_START));
		}
		return period;
	}

	/**
	 * 计算10:00~16:00之间的停留时间，单位秒
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	public static int calculateDayLingeringPeriod(int start, int end) {
		int period = 0;
		if (end < DAY_LINGERING_PERIOD_START
				|| start > DAY_LINGERING_PERIOD_END) {
			return period;
		}

		period = Math.min(end, DAY_LINGERING_PERIOD_END)
				- Math.max(start, DAY_LINGERING_PERIOD_START);
		return period;
	}
}
