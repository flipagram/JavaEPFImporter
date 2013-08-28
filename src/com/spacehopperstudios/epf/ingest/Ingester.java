//  
//  Ingester.java
//  epfimporter
//
//  Created by William Shakour on 7 Aug 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package com.spacehopperstudios.epf.ingest;

import java.io.IOException;

import com.spacehopperstudios.epf.SubstringNotFoundException;
import com.spacehopperstudios.epf.parse.V3Parser;

/**
 * @author billy1380
 * 
 */
public interface Ingester {
	void init(String filePath, V3Parser parser, String tablePrefix, String dbHost, String dbUser, String dbPassword, String dbName, String recordDelim,
			String fieldDelim) throws IOException, SubstringNotFoundException;

	/**
	 * Perform a full or incremental ingest, depending on this.parser.exportMode
	 */
	void ingest(boolean skipViolators);

	/**
	 * Perform a full ingest of the file at this.filePath.
	 * 
	 * This is done as follows: 1. Create a new table with a temporary name 2. Populate the new table 3. Drop the old table and rename the new one
	 */
	public void ingestFull(boolean skipKeyViolators/* =False */);

	/**
	 * Resume an interrupted full ingest, continuing from fromRecord.
	 */
	public void ingestFullResume(long fromRecord/* =0 */, boolean skipKeyViolators/* =False */);

	/**
	 * Update the table with the data in the file at filePath.
	 * 
	 * If the file to ingest has < 500,000 records, we do a simple REPLACE operation on the existing table. If it's larger than that, we use the following
	 * 3-step process: 1. Create a temporary table, and populate it exactly as though it were a Full ingest 2. Perform a SQL query which selects all rows in the
	 * old table whose primary keys *don't* match those in the new table, unions the result with all rows in the new table, and writes the resulting set to
	 * another temporary table. 3. Swap out the old table for the new one via a rename (same as for Full ingests) This proves to be much faster for large files.
	 */
	void ingestIncremental(long fromRecord/* =0 */, boolean skipKeyViolators /* =False */);
}
