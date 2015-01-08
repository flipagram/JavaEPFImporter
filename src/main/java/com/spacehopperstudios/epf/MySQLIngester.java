package com.spacehopperstudios.epf;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.spacehopperstudios.database.Connection;

/**
 * MYSQL implementation of the {@link Ingester}.
 */
public class MySQLIngester extends Ingester {


	Parser parser;
	private static final Logger LOGGER = Logger.getLogger(MySQLIngester.class);

	public MySQLIngester(String filePath, String tablePrefix, String dbHost, String dbUser, String dbPassword, String dbName, String recordDelim, String fieldDelim) throws IOException, SubstringNotFoundException {
		super(filePath, tablePrefix, dbHost, dbUser, dbPassword, dbName, recordDelim, fieldDelim);
		parser = new Parser(filePath, Parser.DEFAULT_TYPE_MAP, recordDelim, fieldDelim);
	}

	@Override
	public Parser getParser() {
		return parser;
	}

	/**
	 * Convenience method which returns True if tableName exists in the db, False if not.
	 *
	 * If tableName is null, uses this.getTableName().
	 *
	 * If a connection object is specified, this method uses it and does not close it; if not, it creates one using connect(), uses it, and then closes it.
	 */
	public boolean tableExists(String tableName/* =null */, Connection connection/* =null */) throws SQLException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {

		String exStr = "SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'";

		if (tableName == null) {
			tableName = this.getTableName();
		}

		Connection conn;
		if (connection == null) {
			conn = this.connect();
		} else {
			conn = connection;
		}

		conn.executeQuery(String.format(exStr, this.getDbName(), tableName));
		conn.fetchNextRow(); // this will always be a 1-tuple; the items's value will be 0 or 1
		boolean doesExist = (conn.getCurrentRowInteger("count") == 1);

		if (connection == null) {
			conn.disconnect();
		}

		return doesExist;
	}

	/**
	 * Appropriately escape the contents of a list of records (as returned by the parser) so that there are no illegal characters (e.g. internal quotes) in the
	 * SQL query.
	 *
	 * This is done here rather than in the parser because it uses the literal() method of the connection object.
	 */
	public List<List<String>> escapeRecords(List<List<String>> recordList, Connection connection/* =null */) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		List<List<String>> escapedRecords = new ArrayList<List<String>>();
		for (List<String> aRec : recordList) {
			List<String> escRec = new ArrayList<String>();
			for (String aField : aRec) {
				String escaped = aField.replace("\\'", "'").replace("\\\\", "\\").replace("\\", "\\\\").replace("'", "\\'");
				escRec.add("'" + escaped  + "'");
			}
			escapedRecords.add(escRec);
		}
		return escapedRecords;

	}

	/**
	 * Convenience method for returning the number of columns in tableName.
	 *
	 * If tableName is null, uses this.getTableName().
	 *
	 * If a connection object is specified, this method uses it and does not close it; if not, it creates one using connect(), uses it, and then closes it.
	 */
	public int columnCount(String tableName/* =null */, Connection connection/* =null */) throws SQLException, InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		if (tableName == null) {
			tableName = this.getTableName();
		}

		Connection conn;

		if (connection == null) {
			conn = this.connect();
		} else {
			conn = connection;
		}

		String exStr = String.format("SELECT COUNT(*) AS count FROM information_schema.COLUMNS where TABLE_NAME='%s';", tableName);
		conn.executeQuery(exStr); // cur.execute() returns the number of rows,
		// which for SHOW COLUMNS is the number of columns in the table

		int colCount = conn.getCurrentRowInteger("count");

		if (connection == null) {
			conn.disconnect();
		}

		return colCount;
	}

	/**
	 * Connect to the db and create a table named this.getTableName()_TMP, dropping previous one if it exists.
	 *
	 * Also adds primary key constraint to the new table.
	 */
	public void createTable(String tableName) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		Connection conn = this.connect();

		conn.executeQuery(String.format("DROP TABLE IF EXISTS %s", tableName));
		// create the column name part of the table creation string
		String colDef;
		List<String> lst = new ArrayList<String>();
		for (int i = 0; i < this.getParser().getColumnNames().size(); i++) {
			colDef = this.getParser().getColumnNames().get(i) + " " + this.getParser().getDataTypes().get(i);
			lst.add(colDef);
		}

		String paramStr = Joiner.on(", ").join(lst);
		// paramString now looks like "export_date BIGINT, storefront_id INT, country_code VARCHAR(100)" etc.
		String exStr = String.format("CREATE TABLE %s (%s)", tableName, paramStr);
		conn.executeQuery(exStr); // create the table in the database
		// set the primary key
		conn.disconnect();
		applyPrimaryKeyConstraints(tableName);
	}

	/**
	 * Apply the primary key specified in parser to tableName.
	 */
	public void applyPrimaryKeyConstraints(String tableName) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		List<String> pkLst = this.getParser().getPrimaryKey();

		if (pkLst != null) {
			Connection conn = this.connect();
			String pkStr = Joiner.on(", ").join(pkLst);

			String exStr = String.format("ALTER TABLE %s ADD CONSTRAINT PRIMARY KEY (%s)", tableName, pkStr);
			conn.executeQuery(exStr);
			conn.disconnect();
		}
	}

	/**
	 * A convenience method that just connects, drops tableName if it exists, and disconnects
	 */
	public void dropTable(String tableName) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {

		Connection conn = this.connect();
		conn.executeQuery(String.format("DROP TABLE IF EXISTS %s", tableName));
		conn.disconnect();
	}

	/**
	 * Temporarily rename targetTable, then rename sourceTable to targetTable. If this succeeds, drop the renamed targetTable; otherwise revert it and drop
	 * sourceTable.
	 */
	public void renameAndDrop(String sourceTable, String targetTable) throws NullPointerException, SQLException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {

		Connection conn = this.connect();

		// first, rename the existing "real" table, so we can restore it if something goes wrong
		String targetOld = targetTable + "_old";
		conn.executeQuery(String.format("DROP TABLE IF EXISTS %s", targetOld));
		if (this.tableExists(targetTable, conn)) {
			conn.executeQuery(String.format("ALTER TABLE %s RENAME %s", targetTable, targetOld));
		}
		// now rename the new table to replace the old table
		try {
			conn.executeQuery(String.format("ALTER TABLE %s RENAME %s", sourceTable, targetTable));
		} catch (SQLException e) {
			LOGGER.error("Could not rename tmp table; reverting to original table (if it exists).", e);
			if (this.tableExists(targetOld, conn)) {
				conn.executeQuery(String.format("ALTER TABLE %s RENAME %s", targetOld, targetTable));
			}
		}
		// Drop sourceTable so it's not hanging around
		// drop the old table
		conn.executeQuery(String.format("DROP TABLE IF EXISTS %s", targetOld));
	}

	/**
	 * After incremental ingest data has been written to this.getIncTableName(), union the pruned original table and the new table into a tmp table
	 */
	public void createUnionTable() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {

		Connection conn = this.connect();
		conn.executeQuery(String.format("DROP TABLE IF EXISTS %s", this.getUnionTableName()));
		String exStr = String.format("CREATE TABLE %s %s", this.getUnionTableName(), incrementalUnionString());
		conn.executeQuery(exStr);
		conn.disconnect();
	}

	/**
	 * Creates and returns the appropriate WHERE clause string used when pruning the target table during an incremental ingest
	 */
	public String incrementalWhereClause() {

		List<String> pCols = this.getParser().getPrimaryKey();
		List<String> substrings = new ArrayList<String>();
		for (String aCol : pCols) {
			substrings.add(String.format("%s.%s=%s.%s", this.getTableName(), aCol, this.getIncTableName(), aCol));
		}
		String joinedString = Joiner.on(" AND ").join(substrings);
		String whereClause = String.format("WHERE %s.export_date <= %s.export_date AND %s", this.getTableName(), this.getIncTableName(), joinedString);

		return whereClause;
	}

	/**
	 * Creates and returns the appropriate SELECT statement used when pruning the target table during an incremental ingest
	 */
	public String incrementalSelectString() {

		String whereClause = incrementalWhereClause();
		String selectString = String.format("SELECT * FROM %s WHERE 0 = (SELECT COUNT(*) FROM %s %s)", this.getTableName(), this.getIncTableName(), whereClause);
		return selectString;
	}

	/**
	 * Creates and returns the appropriate UNION string used when merging the pruned table with the temporary incrmental table.
	 *
	 * The ingest and pruning process should preclude any dupes, so we can use ALL, which should be faster.
	 */
	public String incrementalUnionString() {

		String selectString = incrementalSelectString();
		String unionString = String.format("IGNORE SELECT * FROM %s UNION ALL %s", this.getIncTableName(), selectString);

		return unionString;
	}
}
