/**
 * 
 */
package com.spacehopperstudios.epf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Charsets;

/**
 * @author billy1380
 *
 */
public class Parser {
    /*
    Parses an EPF file.
    
    During initialization, all the file db metadata is stored, and the
    file seek position is set to the beginning of the first data record.
    The Parser object can then be used directly by an Ingester to create
    and populate the table.
    
    typeMap is a dictionary mapping datatype strings in the file to corresponding
    types for the database being used. The default map is for MySQL.
    */
    private static String commentChar = "#";
    private String recordDelim = "\u0002\n";
    private String fieldDelim = "\u0001";
    private static String primaryKeyTag = "primaryKey:";
    private static String dataTypesTag = "dbTypes:";
    private static String exportModeTag = "exportMode:";
    private static String recordCountTag = "recordsWritten:";
    private List<String> numberTypes;
    private List<String> dateTypes;
    public List<String> columnNames;
    public List<String> primaryKey;
    public List<String> dataTypes;
    private List<Integer> dateColumns;
    private Map<String, String> dataTypeMap;
    public String exportMode;
    private List<Integer> numberColumns;
    private Map<String, String> typeMap;
    public long recordsExpected; 
    long latestRecordNum;
    
    private RandomAccessFile eFile;
    
    public static Map<String, String> DEFAULT_TYPE_MAP;
    
    static {
    	DEFAULT_TYPE_MAP = new HashMap<String, String>();
    	DEFAULT_TYPE_MAP.put("CLOB","LONGTEXT");
    }
    
    public Parser(String filePath, Map<String, String> typeMap/* ={"CLOB":"LONGTEXT"} */, String recordDelim/* ='\x02\n' */, String fieldDelim/*='\x01'*/) throws IOException, SubstringNotFoundException {
    	dataTypeMap = typeMap;
        numberTypes = Arrays.asList(new String [] {"INTEGER", "INT", "BIGINT", "TINYINT"});
        dateTypes = Arrays.asList(new String [] {"DATE", "DATETIME", "TIME", "TIMESTAMP"});
        columnNames = new ArrayList<String>();
        this.primaryKey = new ArrayList<String>();
        this.dataTypes = new ArrayList<String>();
        this.exportMode = null;
        this.dateColumns = new ArrayList<Integer>(); // fields containing dates need special treatment; we'll cache the indexes here
        this.numberColumns = new ArrayList<Integer>(); // numeric fields don't accept NULL; we'll cache the indexes here to use later
        this.typeMap = null;
        this.recordsExpected = 0;
        this.latestRecordNum = 0;
        this.recordDelim = recordDelim;
        this.fieldDelim = fieldDelim;
        
        this.eFile = new RandomAccessFile(filePath, "r");
        
        		// Seek to the end and parse the recordsWritten line
        this.eFile.seek(eFile.length() -40);
        String str = this.eFile.readLine();
        String [] lst = str.split(Parser.commentChar + Parser.recordCountTag);
        String numStr = lst[0].split(this.recordDelim)[1];
        this.recordsExpected = Integer.parseInt(numStr);
        this.eFile.seek(0); // seek back to the beginning
        // Extract the column names
        String line1 = this.nextRowString(false);
        this.columnNames = this.splitRow(line1, Parser.commentChar);
        
        		// We'll now grab the rest of the header data, without assuming a particular order
        String primStart = Parser.commentChar + Parser.primaryKeyTag;
        String dtStart = Parser.commentChar + Parser.dataTypesTag;
        String exStart = Parser.commentChar + Parser.exportModeTag;
        
        // Grab the next 6 lines, which should include all the header comments
        List<String> firstRows = new ArrayList<String>();
        for (int j = 0; j < 6; j++) {
            firstRows.add(this.nextRowString(false));
            List<String> firstRowsTmp = new ArrayList<String>();
            for (String aRow : firstRows) {
            	if (aRow != null && aRow.length() > 0) {
            		firstRowsTmp.add(aRow); // strip null rows (possible if the file is < 6 rows)
            	}
            }
            firstRows = firstRowsTmp;
        }
        
            		// Loop through the rows, extracting the header info
        for (String aRow : firstRows) {
            if (aRow.startsWith(primStart)) {
                this.primaryKey = this.splitRow(aRow, primStart);
                if (this.primaryKey == null || this.primaryKey.size() == 1 && this.primaryKey.get(0).equals(" ")) {
                	this.primaryKey = new ArrayList<String>();
                }
            } else if (aRow.startsWith(dtStart)) {
                this.dataTypes = this.splitRow(aRow, dtStart);
            } else if (aRow.startsWith(exStart)) {
                this.exportMode = this.splitRow(aRow, exStart).get(0);
            }
        }
        this.eFile.seek(0); // seek back to the beginning

        // Convert any datatypes to mapped counterparts, and cache indexes of date/time types and number types
        for (int j = 0; j < this.dataTypes.size(); j++) {
            String dType = this.dataTypes.get(j);
            if (this.dataTypeMap.get(dType) != null) {
                this.dataTypes.set(j, this.dataTypeMap.get(dType));
            }
            
            if (dateTypes.contains(dType)) {
                this.dateColumns.add(Integer.valueOf(j));
            }
            
            if (this.numberTypes.contains(dType)) {
                this.numberColumns.add(Integer.valueOf(j));
            }
        }
        
        // Build a dictionary of column names to data types
        this.typeMap = new HashMap<String, String>();
        
        for (int i = 0; i < columnNames.size(); i++) {
        	typeMap.put(this.columnNames.get(i), this.dataTypes.get(i));
		}
    }
        
    
    void setSeekPos(long pos/*=0*/) throws IOException {
        /*
        Sets the underlying file's seek position.
        
        This is useful for resuming a partial ingest that was interrupted for some reason.
        */
        this.eFile.seek(pos);
    }
 
    
    long getSeekPos() throws IOException {
        /*
        Gets the underlying file's seek position.
        */
        return this.eFile.getFilePointer();
    }
    

    void seekToRecord(long recordNum) throws IOException {
        /*
        Set the seek position to the beginning of the recordNumth record.
        
        Seeks to the beginning of the file if recordNum <=0,
        or the end if it's greater than the number of records.
        */
        setSeekPos( 0);
        this.latestRecordNum = 0;
        if (recordNum <= 0) {
            return;
        }
        for (long j = 0; j < recordNum; j++) {
            this.advanceToNextRecord();
        }
    }

        
    /**
    Returns (as a string) the next row of data (as delimited by this.recordDelim),
    ignoring comments if ignoreComments is True.
    
    Leaves the delimiters in place.
    
    Unfortunately Python doesn't allow line-based reading with user-supplied line separators
    (http://bugs.python.org/issue1152248), so we use normal line reading and then concatenate
    when we hit 0x02.
    */
    String nextRowString(boolean ignoreComments /* =True */) throws IOException {
        
        List <String> lst = new ArrayList<String>();
        boolean isFirstLine = true;
        while (true) {
            String ln = this.eFile.readLine();
            if (ln == null) { // end of file
                break;
            }
            
            ln = new String(ln.getBytes(), Charsets.UTF_8);
            if (isFirstLine && ignoreComments && !ln.startsWith(Parser.commentChar)){ // comment
                continue;
            }
            
            lst.add(ln);
            if (isFirstLine) {
                isFirstLine = false;
            }
            if (ln.contains(this.recordDelim)) { // last textual line of this record
                break;
            }
        }
        if (lst.size() == 0) {
            return null;
        }
        else {
        	// concatenate the lines into a single string, which is the full content of the row
        	StringBuffer sb = new StringBuffer();
        	for (String string : lst) {
				sb.append(string);
			}
            return sb.toString();
        }
    }
            
            
    void advanceToNextRecord() throws IOException {
        /*
        Performs essentially the same task as nextRowString, but without constructing or returning anything.
        This allows much faster access to a record in the middle of the file.
        */
        while (true){
            String ln = this.eFile.readLine();
            if ( ln  == null) { // end of file
                return;
            }
            if (ln.startsWith(this.commentChar)) { // comment; always skip
                continue;
            }
            
            if (ln.contains(this.recordDelim)) { // last textual line of this record
                break;
            }
        }
            
        this.latestRecordNum += 1;
        }
        
   
    List<String> splitRow(String rowString, String requiredPrefix/* =null */) throws SubstringNotFoundException {
        /*
        Given rowString, strips requiredPrefix and this.recordDelim,
        then splits on this.fieldDelim, returning the resulting list.
        
        If requiredPrefix is not present in the row, throws a SubstringNotFound exception
        */
        if (requiredPrefix != null) {
            int ix = rowString.indexOf(requiredPrefix);
            if (ix != 0) {
                String expl = String.format("Required prefix '%s' was not found in '%s'", requiredPrefix, rowString);
                throw new SubstringNotFoundException( expl );
            }
            rowString = rowString.split(requiredPrefix)[2];
        }
        String str = rowString.split(this.recordDelim)[0];
        return Arrays.asList(str.split(this.fieldDelim));
    }

    /**
    Returns the next row of data as a list, or null if we're out of data.
     * @throws IOException 
     * @throws SubstringNotFoundException 
    */
    List<String> nextRecord() throws IOException, SubstringNotFoundException {
    
        String rowString = nextRowString(true);
        if (rowString != null) {
            this.latestRecordNum += 1; // update the record counter
            List<String> rec = this.splitRow(rowString, null);
            rec = rec.subList(0, this.columnNames.size()); // if there are more data records than column names,
            		// trim any surplus records via a slice
            
            		// replace empty strings with NULL
            for (int i = 0; i < rec.size(); i++) {
                String val = rec.get(i);
                rec.set(i, (val.length() == 0 ? "NULL" : val));
            }
                		// massage dates into MySQL-compatible format.
                		// most date values look like '2009 06 21'; some are '2005-09-06-00:00:00-Etc/GMT'
                		// there are also some cases where there's only a year; we'll pad it out with a bogus month/day
            Pattern yearMatch = Pattern.compile("^\\d\\d\\d\\d$");
            for (Integer j : this.dateColumns) {
                rec.set(j.intValue(), rec.get(j.intValue()).trim().replace(" ", "-").substring(0, 19)); // Include at most the first 19 chars
                if (yearMatch.matcher(rec.get(j.intValue())).matches()) {
                     rec.set(j.intValue(), String.format("%s-01-01", rec.get(j.intValue())));
                }
            }
            return rec;
        }
        else {
            return null;
            }
    }
                
    /**
    Returns the next maxNum records (or fewer if EOF) as a list of lists.
     * @throws SubstringNotFoundException 
     * @throws IOException 
    */
    List<List<String>> nextRecords(long maxNum /*=100*/) throws IOException, SubstringNotFoundException {
        
        List <List<String>> records = new ArrayList<List<String>>();
        for (long j = 0; j < maxNum; j++) {
            List<String> lst = this.nextRecord();
            if (lst == null) {
                break;
            }
            records.add(lst);
        }
        return records;
    }
                
	/**
	 * Returns the next row of data as a dictionary, keyed by the column names.
	 * @throws SubstringNotFoundException 
	 * @throws IOException 
	 */
	Map<String, String> nextRecordDict() throws IOException, SubstringNotFoundException {

		List<String> vals = this.nextRecord();
		if (vals == null) {
			return null;
		} else {
			List<String> keys = this.columnNames;
			Map<String, String> dict = new HashMap<String, String>();
			for (int i = 0; i < keys.size(); i++) {
				dict.put(keys.get(i), vals.get(i));
			}

			return dict;
		}
	}

}
