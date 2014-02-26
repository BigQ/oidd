package com.sanss.oidd.connector.ai;

import java.util.StringTokenizer;

public class AiEventParser {

	protected static final String EVDO_ONLINE_FLAG = "#777";
	protected static final int EVDO_ONLINE_EVENT = 8;
	protected static final String NULL = "null";

	private String mdn;
	private String imsi;
	private String trackDate;
	/**
	 * 1: Power On 2: Power Off 3: Cylic Location Update 4: Location Update 5:
	 * Shift 6: Calling 7: Called 31: SMS Send 32: SMS Receive 8: Evdo Online *
	 */
	private int event;
	private String cell;
	private int sector;
	private String peer;
	private boolean isParsed;

	public void parse(String record) {
		StringTokenizer token = new StringTokenizer(record, ",");

		if (token.countTokens() == 9) {
			cell = token.nextToken();
			sector = Integer.parseInt(token.nextToken());
			token.nextToken();
			imsi = token.nextToken();
			mdn = token.nextToken();
			token.nextToken();
			trackDate = token.nextToken();
			event = Integer.parseInt(token.nextToken());
			peer = token.nextToken();
			if (peer.equals(EVDO_ONLINE_FLAG)) {
				event = EVDO_ONLINE_EVENT;
			} else if (peer.equals(NULL)) {
				peer = "";
			}
			isParsed = true;
		} else {
			isParsed = false;
		}
	}

	public boolean isParsed() {
		return isParsed;
	}

	public String getMdn() {
		return mdn;
	}

	public String getImsi() {
		return imsi;
	}

	public String getTrackDate() {
		return trackDate;
	}

	public int getEvent() {
		return event;
	}

	public String getCell() {
		return cell;
	}

	public int getSector() {
		return sector;
	}

	public String getPeer() {
		return peer;
	}

}
