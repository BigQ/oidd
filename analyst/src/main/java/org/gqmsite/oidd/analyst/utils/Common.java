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
	public static final int CYLIC_FIRST_BONUS_CONDITION = 28800; // before 8:00
																	// am
	public static final int CYLIC_LAST_BONUS = 3600; // 1 hour
	public static final int CYLIC_LAST_BONUS_CONDITION = 72000; // after 20:00
	public static final int MAX_DIFFS = 86399;
}
