//  
//  IngesterBase.java
//  epfimporter
//
//  Created by William Shakour on 28 Aug 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package com.spacehopperstudios.epf.ingest;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.spacehopperstudios.epf.parse.V3Parser;

/**
 * @author billy1380
 * 
 */
public abstract class IngesterBase implements Ingester {

	protected String tableName;

	protected Map<String, String> statusDict;
	protected String filePath;
	protected String fileName;

	protected Date startTime;
	protected Date endTime;

	protected Date abortTime;
	protected boolean didAbort;

	protected long lastRecordIngested;
	protected V3Parser parser;

	protected long lastRecordCheck = 0;
	protected Date lastTimeCheck;

	public void updateStatusDict() {
		this.statusDict.put("fileName", this.fileName);
		this.statusDict.put("filePath", this.filePath);
		this.statusDict.put("lastRecordIngested", Long.toString(this.lastRecordIngested));

		if (this.startTime != null) {
			this.statusDict.put("startTime", this.startTime.toString());
		}

		if (endTime != null) {
			this.statusDict.put("endTime", this.endTime.toString());
		}

		if (abortTime != null) {
			this.statusDict.put("abortTime", this.abortTime.toString());
		}

		this.statusDict.put("didAbort", Boolean.toString(this.didAbort));
	}

	@Override
	public void ingest(boolean skipKeyViolators/* =False */) {

		if ("INCREMENTAL".equals(this.parser.getExportMode())) {
			this.ingestIncremental(0, skipKeyViolators);
		} else {
			this.ingestFull(skipKeyViolators);
		}
	}

	/**
	 * Checks whether recordGap or more records have been ingested since the last check; if so, checks whether timeGap seconds have elapsed since the last
	 * check.
	 * 
	 * If both checks pass, returns this.lastRecordIngested; otherwise returns null.
	 */
	protected long checkProgress(int recordGap/* =5000 */, long timeGap/* =datetime.timedelta(0, 120, 0) */) {

		if (this.lastRecordIngested - this.lastRecordCheck >= recordGap) {
			Date t = new Date();
			if (t.getTime() - this.lastTimeCheck.getTime() >= timeGap) {
				this.lastTimeCheck = t;
				this.lastRecordCheck = this.lastRecordIngested;
				
				return this.lastRecordCheck;
			}
		}

		return 0;
	}

	protected void initTableName(String filePath, String tablePrefix) {

		this.filePath = filePath;
		this.fileName = (new File(filePath)).getName();
		String pref = tablePrefix == null || tablePrefix.length() == 0 ? "" : String.format("%s_", tablePrefix);
		this.tableName = (pref + this.fileName).replace("-", "_"); // hyphens aren't allowed in table names

		if (this.tableName.contains(".")) {
			this.tableName = this.tableName.split(".", -1)[0];
		}
	}

	protected void initVariables(V3Parser parser) {
		this.lastRecordIngested = -1;
		
		this.parser = (V3Parser) parser;

		this.startTime = null;
		this.endTime = null;
		this.abortTime = null;
		this.didAbort = false;
		this.statusDict = new HashMap<String, String>();

		this.updateStatusDict();

		this.lastRecordCheck = 0;
		this.lastTimeCheck = new Date();
	}
}
