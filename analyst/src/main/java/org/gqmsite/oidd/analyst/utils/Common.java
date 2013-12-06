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

	public static final int LINGER_CURRENTNIGHT_END = 21600;
	public static final int LINGER_NEXTNIGHT_START = 72000;
	public static final int LINGER_CURRENTDAY_START = 36000;
	public static final int LINGER_CURRENTDAY_END = 57600;

	/**
	 * 计算 0:00~6:00 之间的停留时间。单位秒
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	public static int calculateCurrentNightLinger(int start, int end) {
		if (start >= LINGER_CURRENTNIGHT_END) {
			return 0;
		}
		return Math.min(LINGER_CURRENTNIGHT_END, end) - start;
	}

	/**
	 * 计算 10:00~16:00 之间的停留时间。单位秒
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	public static int calculateCurrentDayLinger(int start, int end) {
		if (end < LINGER_CURRENTDAY_START || start > LINGER_CURRENTDAY_END) {
			return 0;
		}

		return Math.min(end, LINGER_CURRENTDAY_END)
				- Math.max(start, LINGER_CURRENTDAY_START);
	}

	/**
	 * 计算 20:00~0:00 之间的停留时间。单位秒
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	public static int calculateNextNightLinger(int start, int end) {
		if (end <= LINGER_NEXTNIGHT_START) {
			return 0;
		}
		return end - Math.max(start, LINGER_NEXTNIGHT_START);
	}
}
