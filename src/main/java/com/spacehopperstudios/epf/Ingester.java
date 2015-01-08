/**
 * 
 */
package com.spacehopperstudios.epf;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.spacehopperstudios.database.Connection;

/**
 * Used to ingest an EPF file into a MySQL database.
 */
public abstract class Ingester {

	public static final String DATETIME_FORMAT = "%y-%m-%d %H:%M:%S";

	private static final Logger LOGGER = Logger.getLogger(Ingester.class);

	// MySQLdb turns MySQL warnings into python warnings, whose behavior is somewhat arcane
	// (as compared with python exceptions.
	// By default, turn all warnings into exceptions
	// warnings.filterwarnings('error');
	// Supress warnings that occur when we do a 'DROP TABLE IF EXISTS'; we expect these,
	// so there's no point in cluttering up the output with them.
	// warnings.filterwarnings('ignore', 'Unknown table.*');

	private String filePath;
	private String fileName;
	private String tableName;
	private String tmpTableName;
	private String incTableName;
	private String unionTableName;
	private String dbHost;
	private String dbUser;
	private String dbPassword;
	private String dbName;
	private long lastRecordIngested;
	private Date startTime;
	private Date endTime;
	private Date abortTime;
	private boolean didAbort;
	private Map<String, String> statusDict;
	private long lastRecordCheck = 0;
	private Date lastTimeCheck;

	public Ingester(String filePath, String tablePrefix/* =null */, String dbHost/* ='localhost' */, String dbUser/* ='epfimporter' */, String dbPassword/*
																																						 * ='epf123'
																																						 */,
			String dbName/* ='epf' */, String recordDelim/* ='\x02\n' */, String fieldDelim/* ='\x01' */) throws IOException, SubstringNotFoundException {
		/*
        */
		this.filePath = filePath;
		this.fileName = (new File(filePath)).getName();
		String pref = tablePrefix == null ? "" : String.format("%s_", tablePrefix);
		this.tableName = (pref + this.fileName).replace("-", "_"); // hyphens aren't allowed in table names

		if (this.tableName.contains(".")) {
			this.tableName = this.tableName.split(".", -1)[0];
		}

		this.tmpTableName = this.tableName + "_tmp";
		this.incTableName = this.tableName + "_inc"; // used during incremental ingests
		this.unionTableName = this.tableName + "_un"; // used during incremental ingests
		this.dbHost = dbHost;
		this.dbUser = dbUser;
		this.dbPassword = dbPassword;
		this.dbName = dbName;
		this.lastRecordIngested = -1;

		this.setStartTime(null);
		this.endTime = null;
		this.abortTime = null;
		this.didAbort = false;
		this.statusDict = new HashMap<String, String>();
		this.updateStatusDict();
		this.lastRecordCheck = 0;
		this.lastTimeCheck = new Date();
	}

	public void updateStatusDict() {
		this.statusDict.put("fileName", this.fileName);
		this.statusDict.put("filePath", this.filePath);
		this.statusDict.put("lastRecordIngested", Long.toString(this.lastRecordIngested));

		if (this.getStartTime() != null) {
			this.statusDict.put("startTime", this.getStartTime().toString());
		}

		if (endTime != null) {
			this.statusDict.put("endTime", this.endTime.toString());
		}

		if (abortTime != null) {
			this.statusDict.put("abortTime", this.abortTime.toString());
		}

		this.statusDict.put("didAbort", Boolean.toString(this.didAbort));
	}

	/**
	 * Perform a full or incremental ingest, depending on getParser().exportMode
	 */
	public void ingest(boolean skipKeyViolators/* =False */) throws IOException, SubstringNotFoundException, SQLException, NullPointerException,
			InstantiationException, IllegalAccessException, ClassNotFoundException {

		if ("INCREMENTAL".equals(getParser().getExportMode())) {
			this.ingestIncremental(0, skipKeyViolators);
		} else {
			this.ingestFull(skipKeyViolators);
		}
	}

	/**
	 * Perform a full ingest of the file at this.filePath.
	 * 
	 * This is done as follows: 1. Create a new table with a temporary name 2. Populate the new table 3. Drop the old table and rename the new one
	 */
	public void ingestFull(boolean skipKeyViolators/* =False */) throws IOException, SubstringNotFoundException, SQLException, NullPointerException,
			InstantiationException, IllegalAccessException, ClassNotFoundException {

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("Beginning full ingest of %s (%d records)", this.tableName, getParser().getRecordsExpected()));
		}

		this.setStartTime(new Date());
		try {
			createTable(this.tmpTableName);
			populateTable(this.tmpTableName, 0, false, skipKeyViolators);
			renameAndDrop(this.tmpTableName, this.tableName);
		} catch (SQLException e) {
			LOGGER.error(String.format("Fatal error encountered while ingesting '%s'", this.filePath), e);
			LOGGER.error(String.format("Last record ingested before failure: %d", this.lastRecordIngested));
			this.abortTime = new Date();
			this.didAbort = true;
			this.updateStatusDict();
			throw e; // re-raise the exception
		}

		// ingest completed
		this.endTime = new Date();
		this.updateStatusDict();

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("Full ingest of %s took %d", this.tableName, this.endTime.getTime() - this.getStartTime().getTime()));
		}
	}

	public abstract void createTable(String tableName) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException,
			ClassNotFoundException;

	public abstract Parser getParser();

	/**
	 * Resume an interrupted full ingest, continuing from fromRecord.
	 */
	public void ingestFullResume(long fromRecord/* =0 */, boolean skipKeyViolators/* =False */) throws IOException, SubstringNotFoundException, SQLException,
			InstantiationException, IllegalAccessException, ClassNotFoundException {

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("Resuming full ingest of %s (%d records)", this.tableName, getParser().getRecordsExpected()));
		}

		this.lastRecordIngested = fromRecord - 1;
		this.setStartTime(new Date());

		try {
			populateTable(this.tmpTableName, fromRecord, false, skipKeyViolators);
			renameAndDrop(this.tmpTableName, this.tableName);
		} catch (SQLException e) {
			// LOGGER.error("Error %d: %s", e.args[0], e.args[1])
			LOGGER.error(String.format("Error encountered while ingesting '%s'", this.filePath));
			LOGGER.error(String.format("Last record ingested before failure: %d", this.lastRecordIngested));
			throw e; // re-raise the exception
		}

		endTime = new Date();
		long ts = this.endTime.getTime() - this.getStartTime().getTime();

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("Resumed full ingest of %s took %d", this.tableName, ts));
		}
	}

	public abstract void renameAndDrop(String sourceTable, String targetTable) throws NullPointerException, SQLException, InstantiationException,
			IllegalAccessException, ClassNotFoundException;

	/**
	 * Update the table with the data in the file at filePath.
	 * 
	 * If the file to ingest has < 500,000 records, we do a simple REPLACE operation on the existing table. If it's larger than that, we use the following
	 * 3-step process: 1. Create a temporary table, and populate it exactly as though it were a Full ingest 2. Perform a SQL query which selects all rows in the
	 * old table whose primary keys *don't* match those in the new table, unions the result with all rows in the new table, and writes the resulting set to
	 * another temporary table. 3. Swap out the old table for the new one via a rename (same as for Full ingests) This proves to be much faster for large files.
	 */
	public void ingestIncremental(long fromRecord/* =0 */, boolean skipKeyViolators /* =False */) throws NullPointerException, SQLException, IOException,
			SubstringNotFoundException, InstantiationException, IllegalAccessException, ClassNotFoundException {

		if (!this.tableExists(this.tableName, null)) {
			// The table doesn't exist in the db; this can happen if the full ingest
			// in which the table was added wasn't performed.
			LOGGER.warn(String.format("Table '%s' does not exist in the database; skipping", this.tableName));
		} else {
			int tableColCount = this.columnCount(null, null);
			int fileColCount = getParser().getColumnNames().size();

			assert (tableColCount <= fileColCount); // It's possible for the existing table
			// to have fewer columns than the file we're importing, but it should never have more.

			if (fileColCount > tableColCount) { // file has "extra" columns
				LOGGER.warn("File contains additional columns not in the existing table. These will not be imported.");
				getParser().setColumnNames(getParser().getColumnNames().subList(0, tableColCount)); // trim the columnNames
				// to equal those in the existing table. This will result in the returned records
				// also being sliced.
			}

			String s = (fromRecord > 0 ? "Resuming" : "Beginning");
			LOGGER.info(String.format("%s incremental ingest of %s (%d records)", s, this.tableName, getParser().getRecordsExpected()));
			this.setStartTime(new Date());

			// Different ingest techniques are faster depending on the size of the input.
			// If there are a large number of records, it's much faster to do a prune-and-merge technique;
			// for fewer records, it's faster to update the existing table.
			try {
				if (getParser().getRecordsExpected() < 500000) { // update table in place
					populateTable(this.tableName, fromRecord, true, skipKeyViolators);
				} else { // Import as full, then merge the proper records into a new table
					createTable(this.incTableName);
					LOGGER.info("Populating temporary table...");
					populateTable(this.incTableName, 0, false, skipKeyViolators);
					LOGGER.info("Creating merged table...");
					createUnionTable();
					dropTable(this.incTableName);
					LOGGER.info("Applying primary key constraints...");
					applyPrimaryKeyConstraints(this.unionTableName);
					renameAndDrop(this.unionTableName, this.tableName);
				}

			} catch (SQLException e) {
				// LOGGER.error("Error %d: %s", e.args[0], e.args[1])
				LOGGER.error(String.format("Fatal error encountered while ingesting '%s'", this.filePath));
				LOGGER.error(String.format("Last record ingested before failure: %d", this.lastRecordIngested));
				this.abortTime = new Date();
				this.didAbort = true;
				this.updateStatusDict();
				throw e; // re-raise the exception
			}

			// ingest completed
			this.endTime = new Date();
			long ts = this.endTime.getTime() - this.getStartTime().getTime();

			if (LOGGER.isInfoEnabled()) {
				LOGGER.info(String.format("Incremental ingest of %s took %d", this.tableName, ts));
			}
		}

		this.updateStatusDict();
	}

	public abstract boolean tableExists(String tableName/* =null */, Connection connection/* =null */) throws SQLException, InstantiationException,
			IllegalAccessException, ClassNotFoundException;

	public abstract int columnCount(String tableName/* =null */, Connection connection/* =null */) throws SQLException, InstantiationException, IllegalAccessException,
			ClassNotFoundException;

	public abstract void applyPrimaryKeyConstraints(String tableName) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException,
			ClassNotFoundException;

	public abstract void dropTable(String tableName) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException;

	public abstract void createUnionTable() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException;

	/**
	 * Establish a connection to the database, returning the connection object.
	 */
	public Connection connect() throws InstantiationException, IllegalAccessException, ClassNotFoundException {

		return new Connection(dbHost, dbName, dbUser, dbPassword);
	}

	public abstract List<List<String>> escapeRecords(List<List<String>> recordList, Connection connection/* =null */) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException;

	/**
	 * Populate tableName with data fetched by the parser, first advancing to resumePos.
	 * 
	 * For Full imports, if skipKeyViolators is True, any insertions which would violate the primary key constraint will be skipped and won't log errors.
	 */
	private void populateTable(String tableName, long resumeNum/* =0 */, boolean isIncremental/* =False */, boolean skipKeyViolators/* =False */)
			throws SQLException, IOException, SubstringNotFoundException, InstantiationException, IllegalAccessException, ClassNotFoundException {

		// REPLACE is a MySQL extension which inserts if the key is new, or deletes and inserts if the key is a duplicate
		String commandString = (isIncremental ? "REPLACE" : "INSERT");
		String ignoreString = (skipKeyViolators && !isIncremental ? "IGNORE" : "");
		String exStrTemplate = "%s %s INTO %s %s VALUES %s";
		String colNamesStr = String.format("(%s)", Joiner.on(", ").join(getParser().getColumnNames()));

		getParser().seekToRecord(resumeNum); // advance to resumeNum
		Connection conn = this.connect();

		while (true) {
			// By default, we concatenate 200 inserts into a single INSERT statement.
			// a large batch size per insert improves performance, until you start hitting max_packet_size issues.
			// If you increase MySQL server's max_packet_size, you may get increased performance by increasing maxNum
			List<List<String>> records = getParser().nextRecords(200);
			if (records == null || records.size() == 0) {
				break;
			}

			List<List<String>> escapedRecords = escapeRecords(records, null); // This will sanitize the records

			List<String> stringList = new ArrayList<String>();
			for (List<String> aRecord : escapedRecords) {
				stringList.add(String.format("(%s)", Joiner.on(", ").join(aRecord)));
			}

			String colVals = new String(Joiner.on(", ").join(stringList).getBytes(), Charsets.UTF_8);
			String exStr = String.format(exStrTemplate, commandString, ignoreString, tableName, colNamesStr, colVals);
			// unquote NULLs
			exStr = exStr.replace("'NULL'", "NULL");
			exStr = exStr.replace("'null'", "NULL");

			try {
				conn.executeQuery(exStr);
			} catch (SQLException e) {
				LOGGER.error(String.format("Error occured executing: %s", exStr), e);
				// } catch (SQLIntegrityConstraintViolationException e) {
				// This is likely a primary key constraint violation; should only be hit if skipKeyViolators is False
			}

			this.lastRecordIngested = getParser().getLatestRecordNum();
			long recCheck = checkProgress(5000, 120 * 1000);

			if (recCheck != 0) {
				if (LOGGER.isInfoEnabled()) {
					LOGGER.info(String.format("...at record %d...", recCheck));
				}
			}
		}

		conn.disconnect();
	}

	/**
	 * Checks whether recordGap or more records have been ingested since the last check; if so, checks whether timeGap seconds have elapsed since the last
	 * check.
	 * 
	 * If both checks pass, returns this.lastRecordIngested; otherwise returns null.
	 */
	private long checkProgress(int recordGap/* =5000 */, long timeGap/* =datetime.timedelta(0, 120, 0) */) {

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

	public String getFilePath() {
		return filePath;
	}

	public String getFileName() {
		return fileName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getTmpTableName() {
		return tmpTableName;
	}

	public String getIncTableName() {
		return incTableName;
	}

	public String getUnionTableName() {
		return unionTableName;
	}

	public String getDbHost() {
		return dbHost;
	}

	public String getDbUser() {
		return dbUser;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	public String getDbName() {
		return dbName;
	}

	public long getLastRecordIngested() {
		return lastRecordIngested;
	}

	public Date getEndTime() {
		return endTime;
	}

	public Date getAbortTime() {
		return abortTime;
	}

	public boolean isDidAbort() {
		return didAbort;
	}

	public Map<String, String> getStatusDict() {
		return statusDict;
	}

	public long getLastRecordCheck() {
		return lastRecordCheck;
	}

	public Date getLastTimeCheck() {
		return lastTimeCheck;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}
}