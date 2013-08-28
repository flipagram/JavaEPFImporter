/**
 * 
 */
package com.spacehopperstudios.epf.parse;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.google.common.base.Charsets;
import com.spacehopperstudios.epf.SubstringNotFoundException;

/**
 * Parses an EPF file.
 * 
 * During initialization, all the file db metadata is stored, and the file seek position is set to the beginning of the first data record. The Parser object can
 * then be used directly by an Ingester to create and populate the table.
 * 
 * typeMap is a dictionary mapping datatype strings in the file to corresponding types for the database being used. The default map is for MySQL.
 */
public class V3Parser {

	private static final Logger LOGGER = Logger.getLogger(V3Parser.class);

	private static final String PRIMARY_KEY_TAG = "primaryKey:";
	private static final String DATA_TYPES_TAG = "dbTypes:";
	private static final String EXPORT_MODE_TAG = "exportMode:";
	private static final String RECORD_COUNT_TAG = "recordsWritten:";

	private List<String> numberTypes;
	private List<String> dateTypes;
	private List<Integer> dateColumns;
	private Map<String, String> dataTypeMap;
	private List<Integer> numberColumns;
	// private Map<String, String> typeMap;

	private String recordDelim = "\u0002\n";
	private String fieldDelim = "\u0001";
	private String commentChar = "#";

	private long recordsExpected;
	private long latestRecordNum;
	private String exportMode;
	private List<String> columnNames;
	private List<String> primaryKey;
	private List<String> dataTypes;

	private RandomAccessFile eFile;

	public long getRecordsExpected() {
		return recordsExpected;
	}

	public long getLatestRecordNum() {
		return latestRecordNum;
	}

	public String getExportMode() {
		return exportMode;
	}

	public List<String> getColumnNames() {
		return columnNames;
	}

	public void setColumnNames(List<String> value) {
		columnNames = value;
	}

	public List<String> getPrimaryKey() {
		return primaryKey;
	}

	public List<String> getDataTypes() {
		return dataTypes;
	}

	public static Map<String, String> DEFAULT_TYPE_MAP;

	static {
		DEFAULT_TYPE_MAP = new HashMap<String, String>();
		DEFAULT_TYPE_MAP.put("CLOB", "LONGTEXT");
	}

	public void init(String filePath, Map<String, String> typeMap/* ={"CLOB":"LONGTEXT"} */, String recordDelim/* ='\x02\n' */, String fieldDelim/* ='\x01' */)
			throws IOException, SubstringNotFoundException {
		dataTypeMap = typeMap;
		numberTypes = Arrays.asList(new String[] { "INTEGER", "INT", "BIGINT", "TINYINT" });
		dateTypes = Arrays.asList(new String[] { "DATE", "DATETIME", "TIME", "TIMESTAMP" });
		columnNames = new ArrayList<String>();
		this.primaryKey = new ArrayList<String>();
		this.dataTypes = new ArrayList<String>();
		this.exportMode = null;
		this.dateColumns = new ArrayList<Integer>(); // fields containing dates need special treatment; we'll cache the indexes here
		this.numberColumns = new ArrayList<Integer>(); // numeric fields don't accept NULL; we'll cache the indexes here to use later
		// this.typeMap = null;
		this.recordsExpected = 0;
		this.latestRecordNum = 0;
		this.recordDelim = recordDelim;
		this.fieldDelim = fieldDelim;

		this.eFile = new RandomAccessFile(filePath, "r");

		// Seek to the end and parse the recordsWritten line
		this.eFile.seek(eFile.length() - 40);
		byte[] b = new byte[40];
		this.eFile.read(b);
		String str = new String(b, Charsets.UTF_8);
		String[] lst = str.split(this.commentChar + V3Parser.RECORD_COUNT_TAG, -1);
		String numStr = lst[lst.length - 1].split(this.recordDelim, -1)[0];
		this.recordsExpected = Integer.parseInt(numStr);
		this.eFile.seek(0); // seek back to the beginning
		// Extract the column names
		String line1 = this.nextRowString(false);
		this.columnNames = this.splitRow(line1, this.commentChar);

		// We'll now grab the rest of the header data, without assuming a particular order
		String primStart = this.commentChar + V3Parser.PRIMARY_KEY_TAG;
		String dtStart = this.commentChar + V3Parser.DATA_TYPES_TAG;
		String exStart = this.commentChar + V3Parser.EXPORT_MODE_TAG;

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

		// // Build a dictionary of column names to data types
		// this.typeMap = new HashMap<String, String>();
		//
		// for (int i = 0; i < columnNames.size(); i++) {
		// typeMap.put(this.columnNames.get(i), this.dataTypes.get(i));
		// }
	}

	/**
	 * Sets the underlying file's seek position.
	 * 
	 * This is useful for resuming a partial ingest that was interrupted for some reason.
	 */
	public void setSeekPos(long pos/* =0 */) throws IOException {

		this.eFile.seek(pos);
	}

	/**
	 * Gets the underlying file's seek position.
	 */
	public long getSeekPos() throws IOException {
		return this.eFile.getFilePointer();
	}

	/**
	 * Set the seek position to the beginning of the recordNumth record.
	 * 
	 * Seeks to the beginning of the file if recordNum <=0, or the end if it's greater than the number of records.
	 */
	public void seekToRecord(long recordNum) throws IOException {

		setSeekPos(0);
		this.latestRecordNum = 0;

		if (recordNum <= 0) {
			return;
		}

		for (long j = 0; j < recordNum; j++) {
			this.advanceToNextRecord();
		}
	}

	/**
	 * Returns (as a string) the next row of data (as delimited by this.recordDelim), ignoring comments if ignoreComments is True.
	 * 
	 * Leaves the delimiters in place.
	 * 
	 * Unfortunately Python doesn't allow line-based reading with user-supplied line separators (http://bugs.python.org/issue1152248), so we use normal line
	 * reading and then concatenate when we hit 0x02.
	 */
	public String nextRowString(boolean ignoreComments /* =True */) throws IOException {

		List<String> lst = new ArrayList<String>();
		boolean isFirstLine = true;
		while (true) {
			String ln = this.eFile.readLine();
			if (ln == null) { // end of file
				break;
			}

			ln = new String(ln.getBytes(), Charsets.UTF_8);
			ln += "\n"; // add the line end that seems to fall off when calling readLine

			if (isFirstLine && ignoreComments && ln.startsWith(this.commentChar)) { // comment
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
		} else {
			// concatenate the lines into a single string, which is the full content of the row
			StringBuffer sb = new StringBuffer();
			for (String string : lst) {
				sb.append(string);
			}

			return sb.toString();
		}
	}

	/**
	 * Performs essentially the same task as nextRowString, but without constructing or returning anything. This allows much faster access to a record in the
	 * middle of the file.
	 */
	public void advanceToNextRecord() throws IOException {

		while (true) {
			String ln = this.eFile.readLine();

			if (ln == null) { // end of file
				return;
			}

			ln = new String(ln.getBytes(), Charsets.UTF_8);
			ln += "\n"; // add the line end that seems to fall off when calling readLine

			if (ln.startsWith(commentChar)) { // comment; always skip
				continue;
			}

			if (ln.contains(this.recordDelim)) { // last textual line of this record
				break;
			}
		}

		this.latestRecordNum += 1;
	}

	/**
	 * Given rowString, strips requiredPrefix and this.recordDelim, then splits on this.fieldDelim, returning the resulting list.
	 * 
	 * If requiredPrefix is not present in the row, throws a SubstringNotFound exception
	 */
	public List<String> splitRow(String rowString, String requiredPrefix/* =null */) throws SubstringNotFoundException {

		if (requiredPrefix != null) {
			int ix = rowString.indexOf(requiredPrefix);
			if (ix != 0) {
				String expl = String.format("Required prefix '%s' was not found in '%s'", requiredPrefix, rowString);
				throw new SubstringNotFoundException(expl);
			}

			rowString = rowString.split(requiredPrefix, -1)[1];
		}

		String str = rowString.split(this.recordDelim, -1)[0];
		String[] splitStr = str.split(this.fieldDelim, -1);

		List<String> row = null;

		if (splitStr != null && splitStr.length != 0) {
			row = new ArrayList<String>();
			Collections.addAll(row, splitStr);
		}

		return row;
	}

	/**
	 * Returns the next row of data as a list, or null if we're out of data.
	 */
	List<String> nextRecord() throws IOException, SubstringNotFoundException {

		String rowString = nextRowString(true);
		if (rowString != null) {
			this.latestRecordNum += 1; // update the record counter
			List<String> rec = this.splitRow(rowString, null);

			if (rec.size() > this.columnNames.size()) {
				rec = rec.subList(0, this.columnNames.size()); // if there are more data records than column names,
				// trim any surplus records via a slice
			} else if (rec.size() < this.columnNames.size()) {
				LOGGER.warn("Number of fields is less than expected");
			}

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
				String corrected = rec.get(j.intValue()).trim().replace(" ", "-");

				if (corrected.length() > 19) {
					corrected = corrected.substring(0, 19);
				}

				rec.set(j.intValue(), corrected); // Include at most the first 19 chars

				if (yearMatch.matcher(rec.get(j.intValue())).matches()) {
					rec.set(j.intValue(), String.format("%s-01-01", rec.get(j.intValue())));
				}
			}
			return rec;
		} else {
			return null;
		}
	}

	/**
	 * Returns the next maxNum records (or fewer if EOF) as a list of lists.
	 */
	public List<List<String>> nextRecords(long maxNum /* =100 */) throws IOException, SubstringNotFoundException {

		List<List<String>> records = new ArrayList<List<String>>();

		for (long j = 0; j < maxNum; j++) {
			List<String> lst = this.nextRecord();

			if (lst == null || lst.size() == 0) {
				break;
			}

			records.add(lst);
		}

		return records;
	}

	/**
	 * Returns the next row of data as a dictionary, keyed by the column names.
	 */
	public Map<String, String> nextRecordDict() throws IOException, SubstringNotFoundException {

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
