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
 * @author billy1380
 *
 */


class Ingester {
	
	private static final String DATETIME_FORMAT = "%y-%m-%d %H:%M:%S";

	private static final Logger	LOGGER = Logger.getLogger(Ingester.class);

	
    /*
    Used to ingest an EPF file into a MySQL database.
    */
    // MySQLdb turns MySQL warnings into python warnings, whose behavior is somewhat arcane
    // (as compared with python exceptions.
    // By default, turn all warnings into exceptions
//    warnings.filterwarnings('error');
    // Supress warnings that occur when we do a 'DROP TABLE IF EXISTS'; we expect these,
    // so there's no point in cluttering up the output with them.
//    warnings.filterwarnings('ignore', 'Unknown table.*');
    
	   String filePath;
	   String fileName;
	   String tableName;
	   String tmpTableName;
	   String incTableName;
	   String unionTableName;
	   String dbHost;
	   String dbUser;
	   String dbPassword;
	   String dbName;
       long lastRecordIngested;
       Parser parser;
       Date startTime;
       Date endTime;
       Date abortTime;
       boolean didAbort;
       Map<String, String> statusDict;
       long lastRecordCheck = 0;
       Date lastTimeCheck;
	
    public Ingester(
            String filePath, 
            String tablePrefix/* =null */,
            String dbHost/* ='localhost' */,
            String dbUser/*='epfimporter'*/,
            String dbPassword/* ='epf123'*/,
            String dbName/* ='epf'*/,
            String recordDelim/* ='\x02\n'*/,
            String fieldDelim/* ='\x01'*/) throws IOException, SubstringNotFoundException {
        /*
        */
        this.filePath = filePath;
        this.fileName = (new File(filePath)).getName();
        String pref = tablePrefix == null ? "" : String.format("%s_", tablePrefix);
        this.tableName = (pref + this.fileName).replace("-", "_"); // hyphens aren't allowed in table names
        this.tableName = this.tableName.split(".")[0];
        this.tmpTableName = this.tableName + "_tmp";
        this.incTableName = this.tableName + "_inc"; // used during incremental ingests
        this.unionTableName = this.tableName + "_un"; // used during incremental ingests
        this.dbHost = dbHost;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.dbName = dbName;
        this.lastRecordIngested = -1;
        
        this.parser = new Parser(filePath, Parser.DEFAULT_TYPE_MAP, recordDelim, fieldDelim);
        this.startTime = null;
        this.endTime = null;
        this.abortTime = null;
        this.didAbort = false;
        this.statusDict = new HashMap<String, String>();
        this.updateStatusDict();
        this.lastRecordCheck = 0;
        this.lastTimeCheck = new Date();
	}
        
    void updateStatusDict() {
        this.statusDict.put("fileName",this.fileName);
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

        
    void ingest(boolean skipKeyViolators/* =False */) throws IOException, SubstringNotFoundException, SQLException, NullPointerException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        /*
        Perform a full or incremental ingest, depending on this.parser.exportMode
        */
        if ("INCREMENTAL".equals(this.parser.exportMode)) {
            this.ingestIncremental(0, skipKeyViolators);
        } else {
            this.ingestFull(skipKeyViolators);
        }
    }

        
    void ingestFull(boolean skipKeyViolators/*=False*/) throws IOException, SubstringNotFoundException, SQLException, NullPointerException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        /*
        Perform a full ingest of the file at this.filePath.
        
        This is done as follows:
        1. Create a new table with a temporary name
        2. Populate the new table
        3. Drop the old table and rename the new one
        */
        LOGGER.info(String.format("Beginning full ingest of %s (%i records)", this.tableName, this.parser.recordsExpected));
        this.startTime = new Date();
        try {
            this._createTable(this.tmpTableName);
            this._populateTable(this.tmpTableName, 0, false,skipKeyViolators);
            this._renameAndDrop(this.tmpTableName, this.tableName);
        } catch (SQLException e) {
            LOGGER.error(String.format("Fatal error encountered while ingesting '%s'", this.filePath));
            LOGGER.error(String.format("Last record ingested before failure: %d", this.lastRecordIngested));
            this.abortTime = new Date();
            this.didAbort = true;
            this.updateStatusDict();
            throw e; // re-raise the exception
        }
        // ingest completed
        this.endTime = new Date();
        this.updateStatusDict();
        LOGGER.info(String.format("Full ingest of %s took %d", this.tableName, this.endTime.getTime() - this.startTime.getTime()));
    }
        
    
    void ingestFullResume(long fromRecord/*=0*/, boolean skipKeyViolators/*=False*/) throws IOException, SubstringNotFoundException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        /*
        Resume an interrupted full ingest, continuing from fromRecord.
        */
        LOGGER.info(String.format("Resuming full ingest of %s (%i records)", this.tableName, this.parser.recordsExpected));
        this.lastRecordIngested = fromRecord - 1;
        this.startTime = new Date();
        try {
            this._populateTable(this.tmpTableName, fromRecord, false,skipKeyViolators);
            this._renameAndDrop(this.tmpTableName, this.tableName);
        } catch (SQLException e) {
            // LOGGER.error("Error %d: %s", e.args[0], e.args[1])
            LOGGER.error(String.format("Error encountered while ingesting '%s'", this.filePath));
            LOGGER.error(String.format("Last record ingested before failure: %d", this.lastRecordIngested));
            throw e; // re-raise the exception
        }
        endTime = new Date();
        long ts = this.endTime.getTime() - this.startTime.getTime();
        LOGGER.info(String.format("Resumed full ingest of %s took %d", this.tableName, ts));
    }
    

    void ingestIncremental(long fromRecord/*=0*/, boolean skipKeyViolators /*=False*/) throws NullPointerException, SQLException, IOException, SubstringNotFoundException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        /*
        Update the table with the data in the file at filePath.
        
        If the file to ingest has < 500,000 records, we do a simple REPLACE operation
        on the existing table. If it's larger than that, we use the following 3-step process:
        1. Create a temporary table, and populate it exactly as though it were a Full ingest
        2. Perform a SQL query which selects all rows in the old table whose primary keys *don't*
           match those in the new table, unions the result with all rows in the new table, and
           writes the resulting set to another temporary table.
        3. Swap out the old table for the new one via a rename (same as for Full ingests)
        This proves to be much faster for large files.
        */
        if (! this.tableExists(this.tableName, null)){
            // The table doesn't exist in the db; this can happen if the full ingest
            // in which the table was added wasn't performed.
            LOGGER.warn(String.format("Table '%s' does not exist in the database; skipping", this.tableName));
        }
        else {
            int tableColCount = this.columnCount(null, null);
            int fileColCount = this.parser.columnNames.size();
            assert (tableColCount <= fileColCount); // It's possible for the existing table
            // to have fewer columns than the file we're importing, but it should never have more.
            
            if (fileColCount > tableColCount){ // file has "extra" columns
                LOGGER.warn("File contains additional columns not in the existing table. These will not be imported.");
                this.parser.columnNames = this.parser.columnNames.subList(0, tableColCount); // trim the columnNames
                //  to equal those in the existing table. This will result in the returned records
                //  also being sliced.
            }
            String s = (fromRecord > 0 ? "Resuming": "Beginning");
            LOGGER.info(String.format("%s incremental ingest of %s (%d records)", s, this.tableName, this.parser.recordsExpected));
            this.startTime = new Date();
            
            // Different ingest techniques are faster depending on the size of the input.
            // If there are a large number of records, it's much faster to do a prune-and-merge technique;
            // for fewer records, it's faster to update the existing table.
            try{
                if (this.parser.recordsExpected < 500000) { // update table in place
                    this._populateTable(this.tableName,
                                    fromRecord, 
                                    true, 
                                    skipKeyViolators);
                }
                else{ // Import as full, then merge the proper records into a new table
                    this._createTable(this.incTableName);
                    LOGGER.info("Populating temporary table...");
                    this._populateTable(this.incTableName, 0, false, skipKeyViolators);
                    LOGGER.info("Creating merged table...");
                    this._createUnionTable();
                    this._dropTable(this.incTableName);
                    LOGGER.info("Applying primary key constraints...");
                    this._applyPrimaryKeyConstraints(this.unionTableName);
                    this._renameAndDrop(this.unionTableName, this.tableName);
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
            long ts = this.endTime.getTime() - this.startTime.getTime();
            LOGGER.info(String.format("Incremental ingest of %s took %d", this.tableName, ts));
        }
        this.updateStatusDict();
    }
                
        
    Connection connect() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        /*
        Establish a connection to the database, returning the connection object.
        */
        return new Connection(dbHost, dbName, dbUser, dbPassword);
    }
            
    
    boolean tableExists(String tableName/*=null*/, Connection connection/*=null*/) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        /*
        Convenience method which returns True if tableName exists in the db, False if not.
        
        If tableName is null, uses this.tableName.
        
        If a connection object is specified, this method uses it and does not close it;
        if not, it creates one using connect(), uses it, and then closes it.
        */
        
        String exStr = "SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema = %s AND table_name = %s";
                
        if (tableName == null) {
        	tableName = this.tableName;
        }
        
        Connection conn;
        if (connection == null) {
        	conn = this.connect();
        } else {
        	conn = connection;
        }
        
        conn.executeQuery(String.format(exStr, this.dbName, tableName));
        conn.fetchNextRow(); // this will always be a 1-tuple; the items's value will be 0 or 1
        boolean doesExist = conn.getCurrentRowInteger("count") == 1;
        
        if (connection == null) {
            conn.disconnect();
        }
        
        return doesExist;
    }
            
    
    int columnCount(String tableName/*=null*/, Connection connection/*=null*/) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        /*
        Convenience method for returning the number of columns in tableName.
        
        If tableName is null, uses this.tableName.
        
        If a connection object is specified, this method uses it and does not close it;
        if not, it creates one using connect(), uses it, and then closes it.
        */
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
        conn.executeQuery(exStr); // cur.execute() returns the number of rows,
        //  which for SHOW COLUMNS is the number of columns in the table
        
        int colCount = conn.getCurrentRowInteger("count");
        
        if (connection == null) {
            conn.disconnect();
        }
        
        return colCount;
    }

    
    void _createTable(String tableName) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        /*
        Connect to the db and create a table named this.tableName_TMP, dropping previous one if it exists.
        
        Also adds primary key constraint to the new table.
        */
        Connection conn = this.connect();
        
        conn.executeQuery(String.format("DROP TABLE IF EXISTS %s" , tableName));
        // create the column name part of the table creation string
        String colDef;
        List<String> lst = new ArrayList<String>();
        for (int i = 0; i< this.parser.columnNames.size(); i++) {
        	colDef = this.parser.columnNames.get(i) + " " + this.parser.dataTypes.get(i); 
        	lst.add(colDef);
		}
        
        String paramStr = Joiner.on(", ").join(lst);
        // paramString now looks like "export_date BIGINT, storefront_id INT, country_code VARCHAR(100)" etc.
        String exStr = String.format("CREATE TABLE %s (%s)", tableName, paramStr);
        conn.executeQuery(exStr); // create the table in the database
        // set the primary key
        conn.disconnect();
        this._applyPrimaryKeyConstraints(tableName);
    }
        

    void _applyPrimaryKeyConstraints(String tableName) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        /*
        Apply the primary key specified in parser to tableName.
        */
        List<String> pkLst = this.parser.primaryKey;
         
        if (pkLst != null){
            Connection conn = this.connect();
            String pkStr = Joiner.on(", ").join(pkLst);
            
            String exStr = String.format("ALTER TABLE %s ADD CONSTRAINT PRIMARY KEY (%s)", tableName, pkStr);
            conn.executeQuery(exStr);
            conn.disconnect();
        }
    }
        

    List<List<String>> _escapeRecords(List<List<String>> recordList, Connection connection/*=null*/) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        /*
        Appropriately escape the contents of a list of records (as returned by the parser)
        so that there are no illegal characters (e.g. internal quotes) in the SQL query.
        
        This is done here rather than in the parser because it uses the literal() method of the
        connection object.
        */
    	Connection conn;
    	
        if (connection == null) {
        	conn = connect();
        } else {
        	conn = connection;
        }
        List<List<String>> escapedRecords = new ArrayList<List<String>>(); 
        for (List<String> aRec : recordList) {
        	List<String >escRec = new ArrayList<String>();
        	for (String aField : aRec) {
        		// TODO: escape string
        		 escRec.add(aField);
        	}
            escapedRecords.add(escRec);
        }
        return escapedRecords;
    }
        

    void _populateTable(String tableName, long resumeNum/*=0*/, boolean isIncremental/*=False*/, boolean skipKeyViolators/*=False*/) throws SQLException, IOException, SubstringNotFoundException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        /*
        Populate tableName with data fetched by the parser, first advancing to resumePos.
        
        For Full imports, if skipKeyViolators is True, any insertions which would violate the primary key constraint
        will be skipped and won't log errors.
        */
        // REPLACE is a MySQL extension which inserts if the key is new, or deletes and inserts if the key is a duplicate
        String commandString = (isIncremental ? "REPLACE" : "INSERT");
        String ignoreString = (skipKeyViolators && !isIncremental ? "IGNORE" : "");
        String exStrTemplate = "%s %s INTO %s %s VALUES %s";
        String colNamesStr = String.format("(%s)", Joiner.on(", ").join(this.parser.columnNames));
        
        this.parser.seekToRecord(resumeNum); // advance to resumeNum
        Connection conn = this.connect();
        
        while (true) {
            // By default, we concatenate 200 inserts into a single INSERT statement.
            // a large batch size per insert improves performance, until you start hitting max_packet_size issues.
            // If you increase MySQL server's max_packet_size, you may get increased performance by increasing maxNum
           List<List<String>> records = this.parser.nextRecords(200);
            if (records == null) {
                break;
            }
            
            List<List<String>> escapedRecords = this._escapeRecords(records, null); // This will sanitize the records
            
            List<String> stringList = new ArrayList<String>();
            for (List<String> aRecord : escapedRecords) {
            	stringList.add(String.format("(%s)", Joiner.on(", ").join(aRecord)));
            }
            
            String colVals = new String(Joiner.on(", ").join(stringList).getBytes(), Charsets.UTF_8);
            String exStr = String.format(exStrTemplate ,commandString, ignoreString, tableName, colNamesStr, colVals);
            // unquote NULLs
            exStr = exStr.replace("'NULL'", "NULL");
            exStr = exStr.replace("'null'", "NULL");

            try {
                conn.executeQuery(exStr);
            } catch (SQLException e) {
                LOGGER.error("", e);
//            } catch (SQLIntegrityConstraintViolationException e) {
            // This is likely a primary key constraint violation; should only be hit if skipKeyViolators is False
                LOGGER.error("", e);
            }
            
            this.lastRecordIngested = this.parser.latestRecordNum;
            long recCheck = this._checkProgress(5000, 120 * 1000);
            if (recCheck !=  0) {
            	if (LOGGER.isInfoEnabled()) {
            		LOGGER.info(String.format("...at record %i...", recCheck));
            	}
            }
        }

        conn.disconnect();
    }

        
    long _checkProgress(int recordGap/* =5000 */, long timeGap/* =datetime.timedelta(0, 120, 0) */) {
        /*
        Checks whether recordGap or more records have been ingested since the last check;
        if so, checks whether timeGap seconds have elapsed since the last check.
        
        If both checks pass, returns this.lastRecordIngested; otherwise returns null.
        */
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
        

    void _dropTable(String tableName) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        /*A convenience method that just connects, drops tableName if it exists, and disconnects*/
        Connection conn = this.connect();
        conn.executeQuery(String.format("DROP TABLE IF EXISTS %s", tableName));
        conn.disconnect();
    }
        

    void _renameAndDrop(String sourceTable, String targetTable) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        /*
        Temporarily rename targetTable, then rename sourceTable to targetTable.
        If this succeeds, drop the renamed targetTable; otherwise revert it and drop sourceTable.
        */
        Connection conn = this.connect();
        
        // first, rename the existing "real" table, so we can restore it if something goes wrong
        String targetOld = targetTable + "_old";
        conn.executeQuery(String.format("DROP TABLE IF EXISTS %s", targetOld));
        if (this.tableExists(targetTable, conn)) {
            conn.executeQuery(String.format("ALTER TABLE %s RENAME %s" , targetTable, targetOld));
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
        
        
    void _createUnionTable() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        /*
        After incremental ingest data has been written to this.incTableName, union the pruned
        original table and the new table into a tmp table
        */
        Connection conn = this.connect();
        conn.executeQuery(String.format("DROP TABLE IF EXISTS %s", this.unionTableName));
        String exStr = String.format("CREATE TABLE %s %s", this.unionTableName, this._incrementalUnionString());
        conn.executeQuery(exStr);
        conn.disconnect();
    }
        

    String _incrementalWhereClause() {
        /*
        Creates and returns the appropriate WHERE clause string used when pruning the target table
        during an incremental ingest
        */
        List<String> pCols = this.parser.primaryKey;
        List<String> substrings = new ArrayList<String>();
        for (String aCol : pCols) {
        	substrings.add(String.format("%s.%s=%s.%s", this.tableName, aCol, this.incTableName, aCol));
        }
        String joinedString = Joiner.on(" AND ").join(substrings);
        String whereClause = String.format("WHERE %s.export_date <= %s.export_date AND %s", this.tableName, this.incTableName, joinedString);
        
        return whereClause;
    }

        
    String _incrementalSelectString() {
        /*
        Creates and returns the appropriate SELECT statement used when pruning the target table
        during an incremental ingest
        */
        String whereClause = this._incrementalWhereClause();
        String selectString = String.format("SELECT * FROM %s WHERE 0 = (SELECT COUNT(*) FROM %s %s)", this.tableName, this.incTableName, whereClause);
        return selectString;
    }

        
    String _incrementalUnionString() {
        /*
        Creates and returns the appropriate UNION string used when merging the pruned table
        with the temporary incrmental table.
        
        The ingest and pruning process should preclude any dupes, so we can use ALL, which should be faster.
        */
        String selectString = this._incrementalSelectString();
        String unionString = String.format("IGNORE SELECT * FROM %s UNION ALL %s", this.incTableName, selectString);
        return unionString;
    }
}