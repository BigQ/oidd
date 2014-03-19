package com.sanss.oidd.analyst.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Common {

	protected static final SimpleDateFormat sdf_common = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	protected static final SimpleDateFormat sdf_simple = new SimpleDateFormat(
			"yyyyMMdd");
	protected static Calendar calendar = Calendar.getInstance();

	/**
	 * event value
	 * 
	 * 1: Power On 2: Power Off 3: Cylic Location Update 4: Location Update 5:
	 * Shift 6: Calling 7: Called 31: SMS Send 32: SMS Receive 8: x1 Online *
	 */
	public static final int C_EVENT_POWERON = 1;
	public static final int C_EVENT_POWEROFF = 2;
	public static final int C_EVENT_CYLICLOCATIONUPDATE = 3;
	public static final int C_EVENT_LOCATIONUPDATE = 4;
	public static final int C_EVENT_SHIFT = 5;
	public static final int C_EVENT_CALLING = 6;
	public static final int C_EVENT_CALLED = 7;
	public static final int C_EVENT_SMSSEND = 31;
	public static final int C_EVENT_SMSRECEIVE = 32;
	public static final int C_EVENT_X1ONLINE = 8;

	/**
	 * state value
	 * 
	 * 0: online; 1: off-line
	 */
	public static final int C_STATE_ONLINE = 1;
	public static final int C_STATE_OFFLINE = 0;

	/**
	 * counter name
	 * 
	 */
	public static final String C_COUNTER_G_SKIPRECORD = "SKIP_RECORD";
	public static final String C_COUNTER_SKIPRECORD_DUP = "duplicated";
	public static final String C_COUNTER_SKIPRECORD_UNKOWN = "unknown.mdn";
	public static final String C_COUNTER_SKIPRECORD_ILLMDN = "illegal.mdn";
	public static final String C_COUNTER_SKIPRECORD_ILLIMSI = "illegal.imsi";
	public static final String C_COUNTER_SKIPRECORD_ERROR = "error";

	/**
	 * others
	 */
	public static final String C_V_UNKNOWN_MDN = "0";
	public static final int C_V_ILLEGAL_MDN_LEN = 11;
	public static final String C_V_ILLEGAL_IMSI = "0";
	
	public static final int C_V_EVENT_MIN_INTERVAL = 120; // 2mins
	public static final int C_V_EVENT_CYLIC_BONUS = 1800; // 30mins
	public static final int C_V_EVENT_CYCLE_FULL = 3600; // 60mins
	public static final int C_V_EVENT_MAX_INTERVAL = 5400; // 90mins
	public static final int C_V_SERVICE_PEAK_START = 28800; // 8am
	public static final int C_V_SERVICE_PEAK_END = 75600; // 9pm
	public static final int C_V_LINGER_PEAK_THRED = 1680;// 28mins
	public static final int C_V_LINGER_IDLE_THRED = 1800;// 30mins
	public static final int C_V_SECONDSINDAY_MAX = 86399;

	/**
	 * convert event to state
	 * 
	 * @param event
	 * @return
	 */
	public static final int convertEvent2State(int event) {
		return event != C_EVENT_POWEROFF ? C_STATE_ONLINE : C_STATE_OFFLINE;
	}

	/**
	 * calculate the seconds in the day.
	 * 
	 * @param dateStr
	 *            pattern like yyyy-MM-dd HH:mm:ss
	 * @return
	 * @throws ParseException
	 */
	public static final int getSecondsInDay(String dateStr)
			throws ParseException {
		calendar.setTime(sdf_common.parse(dateStr));
		return calendar.get(Calendar.SECOND) + calendar.get(Calendar.MINUTE)
				* 60 + calendar.get(Calendar.HOUR_OF_DAY) * 3600;
	}
	
	public static final int getSeconds(String dateStr){
		try {
			return (int) (sdf_common.parse(dateStr).getTime() / 1000);
		} catch (ParseException e) {
			return -1;
		}
	}
	
	/**
	 * format the location expression. the pattern is "cell@sector"
	 * @param cell
	 * @param sector
	 * @return
	 */
	public static final String getLoc(String cell, int sector){
		return new StringBuilder(cell).append("@").append(sector).toString();
	}
}
