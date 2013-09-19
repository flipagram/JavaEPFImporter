/**
 * 
 */
package com.spacehopperstudios.epf.ingest;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.spacehopperstudios.database.Connection;
import com.spacehopperstudios.epf.SubstringNotFoundException;
import com.spacehopperstudios.epf.TimeHelper;
import com.spacehopperstudios.epf.parse.V3Parser;

/**
 * Used to ingest an EPF file into a MySQL database.
 */
class MySqlIngester extends IngesterBase implements Ingester {

	private static final Logger LOGGER = Logger.getLogger(MySqlIngester.class);

	// MySQLdb turns MySQL warnings into python warnings, whose behavior is somewhat arcane
	// (as compared with python exceptions.
	// By default, turn all warnings into exceptions
	// warnings.filterwarnings('error');
	// Supress warnings that occur when we do a 'DROP TABLE IF EXISTS'; we expect these,
	// so there's no point in cluttering up the output with them.
	// warnings.filterwarnings('ignore', 'Unknown table.*');

	private String tmpTableName;
	private String incTableName;
	private String unionTableName;
	private String dbHost;
	private String dbUser;
	private String dbPassword;
	private String dbName;

	@Override
	public void init(String filePath, V3Parser parser, String tablePrefix/* =null */, String dbHost/* ='localhost' */, String dbUser/* ='epfimporter' */,
			String dbPassword/*
							 * ='epf123'
							 */, String dbName/* ='epf' */, String recordDelim/* ='\x02\n' */, String fieldDelim/* ='\x01' */) throws IOException,
			SubstringNotFoundException {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Init started");
		}

		initTableName(filePath, tablePrefix);

		this.tmpTableName = this.tableName + "_tmp";
		this.incTableName = this.tableName + "_inc"; // used during incremental ingests
		this.unionTableName = this.tableName + "_un"; // used during incremental ingests
		this.dbHost = dbHost;
		this.dbUser = dbUser;
		this.dbPassword = dbPassword;
		this.dbName = dbName;

		initVariables(parser);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Init ended");
		}
	}

	/**
	 * Perform a full ingest of the file at this.filePath.
	 * 
	 * This is done as follows: 1. Create a new table with a temporary name 2. Populate the new table 3. Drop the old table and rename the new one
	 */
	public void ingestFull(boolean skipKeyViolators/* =False */) {

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("Beginning full ingest of %s (%d records)", this.tableName, this.parser.getRecordsExpected()));
		}

		this.startTime = new Date();
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
			throw new RuntimeException(e); // re-raise the exception
		} catch (Exception e) {
			throw new RuntimeException(e); // re-raise the exception
		}

		// ingest completed
		this.endTime = new Date();
		this.updateStatusDict();

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("Full ingest of %s took %d", this.tableName, TimeHelper.durationText(startTime, endTime)));
		}
	}

	/**
	 * Resume an interrupted full ingest, continuing from fromRecord.
	 */
	public void ingestFullResume(long fromRecord/* =0 */, boolean skipKeyViolators/* =False */) {

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("Resuming full ingest of %s (%d records)", this.tableName, this.parser.getRecordsExpected()));
		}

		this.lastRecordIngested = fromRecord - 1;
		this.startTime = new Date();

		try {
			populateTable(this.tmpTableName, fromRecord, false, skipKeyViolators);
			renameAndDrop(this.tmpTableName, this.tableName);
		} catch (SQLException e) {
			// LOGGER.error("Error %d: %s", e.args[0], e.args[1])
			LOGGER.error(String.format("Error encountered while ingesting '%s'", this.filePath));
			LOGGER.error(String.format("Last record ingested before failure: %d", this.lastRecordIngested));
			throw new RuntimeException(e); // re-raise the exception
		} catch (Exception e) {
			throw new RuntimeException(e); // re-raise the exception
		}

		endTime = new Date();

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("Resumed full ingest of %s took %s", this.tableName, TimeHelper.durationText(startTime, endTime)));
		}
	}

	public void ingestIncremental(long fromRecord/* =0 */, boolean skipKeyViolators /* =False */) {

		try {
			if (!this.tableExists(this.tableName, null)) {
				// The table doesn't exist in the db; this can happen if the full ingest
				// in which the table was added wasn't performed.
				LOGGER.warn(String.format("Table '%s' does not exist in the database; skipping", this.tableName));
			} else {
				int tableColCount = this.columnCount(null, null);
				int fileColCount = this.parser.getColumnNames().size();

				assert (tableColCount <= fileColCount); // It's possible for the existing table
				// to have fewer columns than the file we're importing, but it should never have more.

				if (fileColCount > tableColCount) { // file has "extra" columns
					LOGGER.warn("File contains additional columns not in the existing table. These will not be imported.");
					this.parser.setColumnNames(this.parser.getColumnNames().subList(0, tableColCount)); // trim the columnNames
					// to equal those in the existing table. This will result in the returned records
					// also being sliced.
				}

				String s = (fromRecord > 0 ? "Resuming" : "Beginning");
				LOGGER.info(String.format("%s incremental ingest of %s (%d records)", s, this.tableName, this.parser.getRecordsExpected()));
				this.startTime = new Date();

				// Different ingest techniques are faster depending on the size of the input.
				// If there are a large number of records, it's much faster to do a prune-and-merge technique;
				// for fewer records, it's faster to update the existing table.
				try {
					if (this.parser.getRecordsExpected() < 500000) { // update table in place
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
					throw new RuntimeException(e); // re-raise the exception
				}

				// ingest completed
				this.endTime = new Date();

				if (LOGGER.isInfoEnabled()) {
					LOGGER.info(String.format("Incremental ingest of %s took %s", this.tableName, TimeHelper.durationText(startTime, endTime)));
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e); // re-raise the exception
		}

		this.updateStatusDict();
	}

	/**
	 * Establish a connection to the database, returning the connection object.
	 */
	private Connection connect() throws InstantiationException, IllegalAccessException, ClassNotFoundException {

		return new Connection(dbHost, dbName, dbUser, dbPassword);
	}

	/**
	 * Convenience method which returns True if tableName exists in the db, False if not.
	 * 
	 * If tableName is null, uses this.tableName.
	 * 
	 * If a connection object is specified, this method uses it and does not close it; if not, it creates one using connect(), uses it, and then closes it.
	 */
	private boolean tableExists(String tableName/* =null */, Connection connection/* =null */) throws SQLException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {

		String exStr = "SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'";

		if (tableName == null) {
			tableName = this.tableName;
		}

		Connection conn;
		if (connection == null) {
			conn = this.connect();
		} else {
			conn = connection;
		}

		executeQuery(conn, String.format(exStr, this.dbName, tableName), 2);

		boolean doesExist = false;

		if (conn.fetchNextRow()) { // this will always be a 1-tuple; the items's value will be 0 or 1
			doesExist = (conn.getCurrentRowInteger("count").intValue() == 1);
		}

		if (connection == null) {
			conn.disconnect();
		}

		return doesExist;
	}

	/**
	 * Convenience method for returning the number of columns in tableName.
	 * 
	 * If tableName is null, uses this.tableName.
	 * 
	 * If a connection object is specified, this method uses it and does not close it; if not, it creates one using connect(), uses it, and then closes it.
	 */
	private int columnCount(String tableName/* =null */, Connection connection/* =null */) throws SQLException, InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		if (tableName == null) {
			tableName = this.tableName;
		}

		Connection conn;

		if (connection == null) {
			conn = this.connect();
		} else {
			conn = connection;
		}

		String exStr = String.format("SELECT COUNT(*) AS count FROM information_schema.COLUMNS where TABLE_NAME='%s';", tableName);
		executeQuery(conn, exStr, 2); // cur.execute() returns the number of rows,
		// which for SHOW COLUMNS is the number of columns in the table

		int colCount = 0;

		if (conn.fetchNextRow()) {
			colCount = conn.getCurrentRowInteger("count").intValue();
		}

		if (connection == null) {
			conn.disconnect();
		}

		return colCount;
	}

	/**
	 * Connect to the db and create a table named this.tableName_TMP, dropping previous one if it exists.
	 * 
	 * Also adds primary key constraint to the new table.
	 */
	private void createTable(String tableName) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		Connection conn = this.connect();

		executeQuery(conn, String.format("DROP TABLE IF EXISTS %s", tableName), 2);
		// create the column name part of the table creation string
		String colDef;
		List<String> lst = new ArrayList<String>();
		for (int i = 0; i < this.parser.getColumnNames().size(); i++) {
			colDef = this.parser.getColumnNames().get(i) + " " + this.parser.getDataTypes().get(i);
			lst.add(colDef);
		}

		String paramStr = Joiner.on(", ").join(lst);
		// paramString now looks like "export_date BIGINT, storefront_id INT, country_code VARCHAR(100)" etc.
		String exStr = String.format("CREATE TABLE %s (%s)", tableName, paramStr);
		executeQuery(conn, exStr, 2); // create the table in the database
		// set the primary key
		conn.disconnect();
		applyPrimaryKeyConstraints(tableName);
	}

	/**
	 * Apply the primary key specified in parser to tableName.
	 */
	private void applyPrimaryKeyConstraints(String tableName) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		List<String> pkLst = this.parser.getPrimaryKey();

		if (pkLst != null) {
			String pkStr = Joiner.on(", ").join(pkLst);

			String exStr = String.format("ALTER TABLE %s ADD CONSTRAINT PRIMARY KEY (%s)", tableName, pkStr);

			Connection conn = this.connect();
			executeQuery(conn, exStr, 2);
			conn.disconnect();
		}
	}

	/**
	 * Appropriately escape the contents of a list of records (as returned by the parser) so that there are no illegal characters (e.g. internal quotes) in the
	 * SQL query.
	 * 
	 * This is done here rather than in the parser because it uses the literal() method of the connection object.
	 */
	private List<List<String>> escapeRecords(List<List<String>> recordList, Connection connection/* =null */) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		List<List<String>> escapedRecords = new ArrayList<List<String>>();
		for (List<String> aRec : recordList) {
			List<String> escRec = new ArrayList<String>();
			for (String aField : aRec) {
				String escaped = aField.replace("\\'", "'").replace("\\\\", "\\").replace("\\", "\\\\").replace("'", "\\'");
				escRec.add("'" + escaped + "'");
			}
			escapedRecords.add(escRec);
		}
		return escapedRecords;

	}

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
		String colNamesStr = String.format("(%s)", Joiner.on(", ").join(this.parser.getColumnNames()));

		this.parser.seekToRecord(resumeNum); // advance to resumeNum
		Connection conn = this.connect();

		while (true) {
			// By default, we concatenate 200 inserts into a single INSERT statement.
			// a large batch size per insert improves performance, until you start hitting max_packet_size issues.
			// If you increase MySQL server's max_packet_size, you may get increased performance by increasing maxNum
			List<List<String>> records = this.parser.nextRecords(200);
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
				executeQuery(conn, exStr, 2);
			} catch (SQLException e) {
				LOGGER.error(String.format("Error occured executing: %s", exStr), e);
				// } catch (SQLIntegrityConstraintViolationException e) {
				// This is likely a primary key constraint violation; should only be hit if skipKeyViolators is False
			}

			this.lastRecordIngested = this.parser.getLatestRecordNum();
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
	 * A convenience method that just connects, drops tableName if it exists, and disconnects
	 */
	private void dropTable(String tableName) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {

		Connection conn = this.connect();
		executeQuery(conn, String.format("DROP TABLE IF EXISTS %s", tableName), 2);
		conn.disconnect();
	}

	/**
	 * Temporarily rename targetTable, then rename sourceTable to targetTable. If this succeeds, drop the renamed targetTable; otherwise revert it and drop
	 * sourceTable.
	 */
	private void renameAndDrop(String sourceTable, String targetTable) throws NullPointerException, SQLException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {

		Connection conn = this.connect();

		// first, rename the existing "real" table, so we can restore it if something goes wrong
		String targetOld = targetTable + "_old";
		executeQuery(conn, String.format("DROP TABLE IF EXISTS %s", targetOld), 2);
		if (this.tableExists(targetTable, conn)) {
			executeQuery(conn, String.format("ALTER TABLE %s RENAME %s", targetTable, targetOld), 2);
		}
		// now rename the new table to replace the old table
		try {
			executeQuery(conn, String.format("ALTER TABLE %s RENAME %s", sourceTable, targetTable), 2);
		} catch (SQLException e) {
			LOGGER.error("Could not rename tmp table; reverting to original table (if it exists).", e);
			if (this.tableExists(targetOld, conn)) {
				executeQuery(conn, String.format("ALTER TABLE %s RENAME %s", targetOld, targetTable), 2);
			}
		}
		// Drop sourceTable so it's not hanging around
		// drop the old table
		executeQuery(conn, String.format("DROP TABLE IF EXISTS %s", targetOld), 2);
	}

	/**
	 * After incremental ingest data has been written to this.incTableName, union the pruned original table and the new table into a tmp table
	 */
	private void createUnionTable() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {

		Connection conn = this.connect();
		executeQuery(conn, String.format("DROP TABLE IF EXISTS %s", this.unionTableName), 2);
		String exStr = String.format("CREATE TABLE %s %s", this.unionTableName, incrementalUnionString());
		executeQuery(conn, exStr, 2);
		conn.disconnect();
	}

	/**
	 * Creates and returns the appropriate WHERE clause string used when pruning the target table during an incremental ingest
	 */
	private String incrementalWhereClause() {

		List<String> pCols = this.parser.getPrimaryKey();
		List<String> substrings = new ArrayList<String>();
		for (String aCol : pCols) {
			substrings.add(String.format("%s.%s=%s.%s", this.tableName, aCol, this.incTableName, aCol));
		}
		String joinedString = Joiner.on(" AND ").join(substrings);
		String whereClause = String.format("WHERE %s.export_date <= %s.export_date AND %s", this.tableName, this.incTableName, joinedString);

		return whereClause;
	}

	/**
	 * Creates and returns the appropriate SELECT statement used when pruning the target table during an incremental ingest
	 */
	private String incrementalSelectString() {

		String whereClause = incrementalWhereClause();
		String selectString = String.format("SELECT * FROM %s WHERE 0 = (SELECT COUNT(*) FROM %s %s)", this.tableName, this.incTableName, whereClause);

		return selectString;
	}

	/**
	 * Creates and returns the appropriate UNION string used when merging the pruned table with the temporary incrmental table.
	 * 
	 * The ingest and pruning process should preclude any dupes, so we can use ALL, which should be faster.
	 */
	private String incrementalUnionString() {

		String selectString = incrementalSelectString();
		String unionString = String.format("IGNORE SELECT * FROM %s UNION ALL %s", this.incTableName, selectString);

		return unionString;
	}

	private void executeQuery(final Connection connection, final String query, final int retryCount) throws SQLException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {

		Connection conn = null;
		int retriesLeft = retryCount;
		boolean done = false;

		if (connection == null) {
			conn = this.connect();
		} else {
			conn = connection;
		}

		while (retriesLeft >= 0 && !done) {
			try {
				if (!conn.isConnected()) {
					if (retriesLeft != retryCount) {
						conn = this.connect();
					}

					conn.connect();
				}
				conn.executeQuery(query);
				done = true;
			} catch (SQLException e) {
				retriesLeft--;

				if (retriesLeft < 0) {
					throw e;
				} else {
					LOGGER.error(String.format("Error occured executing: %s", query), e);
				}
			}
		}
	}
}