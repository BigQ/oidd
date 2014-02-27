package com.sanss.oidd.analyst.utils;

public class Common {

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
	public static final String C_COUNTER_SKIPRECORD_ILLMDN = "illegal.mdn";
	public static final String C_COUNTER_SKIPRECORD_ILLIMSI = "illegal.imsi";
	
	/**
	 * others
	 */
	public static final String C_V_UNKNOWN_MDN = "0";
	public static final String C_V_ILLEGAL_IMSI = "0";
	
	/**
	 * convert event to state
	 * 
	 * @param event
	 * @return
	 */
	public static final int convertEvent2State(int event) {
		return event != C_EVENT_POWEROFF ? C_STATE_ONLINE : C_STATE_OFFLINE;
	}

}
