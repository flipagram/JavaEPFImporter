//
//  TimeHelper.java
//  epfimporter
//
//  Created by William Shakour (billy1380) on 19 Sep 2013.
//  Copyright © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package com.spacehopperstudios.epf;

import java.util.Date;

import org.apache.log4j.helpers.ISO8601DateFormat;

/**
 * @author billy1380
 * 
 */
public class TimeHelper {

	public static String durationText(Date start, Date end) {
		long duration = end.getTime() - start.getTime();

		long hours = duration / (1000 * 60 * 60);
		duration -= hours * 1000 * 60 * 60;

		long minutes = duration / (1000 * 60);
		duration -= minutes * 1000 * 60;

		long seconds = duration / 1000;
		duration -= seconds * 1000;

		return String.format("%dh %dm %ds,%d", hours, minutes, seconds, duration);
	}

	/**
	 * @param endTime
	 * @return
	 */
	public static String timeText(Date time) {
		return (new ISO8601DateFormat()).format(time);
	}
}
