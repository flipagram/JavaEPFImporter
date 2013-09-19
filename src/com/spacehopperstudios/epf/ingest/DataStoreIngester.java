//  
//  DataStoreIngester.java
//  epfimporter
//
//  Created by William Shakour on 7 Aug 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package com.spacehopperstudios.epf.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.CompositeFilter;
import com.google.appengine.api.datastore.Query.CompositeFilterOperator;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.remoteapi.RemoteApiInstaller;
import com.google.appengine.tools.remoteapi.RemoteApiOptions;
import com.spacehopperstudios.epf.SubstringNotFoundException;
import com.spacehopperstudios.epf.TimeHelper;
import com.spacehopperstudios.epf.parse.V3Parser;

/**
 * @author billy1380
 * 
 */
public class DataStoreIngester extends IngesterBase implements Ingester {

	private static final Logger LOGGER = Logger.getLogger(DataStoreIngester.class);

	RemoteApiInstaller installer;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.spacehopperstudios.epf.ingest.Ingester#init(java.lang.String, com.spacehopperstudios.epf.parse.Parser, java.lang.String, java.lang.String,
	 * java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void init(String filePath, V3Parser parser, String tablePrefix, String dbHost, String dbUser, String dbPassword, String dbName, String recordDelim,
			String fieldDelim) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Init started");
		}

		initTableName(filePath, tablePrefix);

		initVariables(parser);

		String[] split = dbHost.split(":");

		RemoteApiOptions options = new RemoteApiOptions().server(split[0], split.length > 1 ? Integer.parseInt(split[1]) : 80).credentials(dbUser, dbPassword);

		installer = new RemoteApiInstaller();

		try {
			installer.install(options);
		} catch (IOException e) {
			installer = null;
			throw new RuntimeException(e); // re-raise the exception
		}

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
			populateTable(this.tableName, 0, false, skipKeyViolators);
		} catch (Exception e) {
			LOGGER.error(String.format("Error encountered while ingesting '%s'", this.filePath));
			LOGGER.error(String.format("Last record ingested before failure: %d", this.lastRecordIngested));
			throw new RuntimeException(e); // re-raise the exception
		} finally {
			if (installer != null) {
				installer.uninstall();
			}
		}

		// ingest completed
		this.endTime = new Date();
		this.updateStatusDict();

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("Full ingest of %s took %s", this.tableName, TimeHelper.durationText(startTime, endTime)));
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
			populateTable(this.tableName, fromRecord, false, skipKeyViolators);
		} catch (Exception e) {
			// LOGGER.error("Error %d: %s", e.args[0], e.args[1])
			LOGGER.error(String.format("Error encountered while ingesting '%s'", this.filePath));
			LOGGER.error(String.format("Last record ingested before failure: %d", this.lastRecordIngested));
			throw new RuntimeException(e); // re-raise the exception
		} finally {
			if (installer != null) {
				installer.uninstall();
			}
		}

		endTime = new Date();

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(String.format("Resumed full ingest of %s took %s", this.tableName, TimeHelper.durationText(startTime, endTime)));
		}
	}

	public void ingestIncremental(long fromRecord/* =0 */, boolean skipKeyViolators /* =False */) {

		try {
			if (!this.tableExists(this.tableName)) {
				// The table doesn't exist in the db; this can happen if the full ingest
				// in which the table was added wasn't performed.
				LOGGER.warn(String.format("Table '%s' does not exist in the database; skipping", this.tableName));
			} else {
				int tableColCount = this.columnCount(null);
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
					} else {
						// for now treat both as the same
						populateTable(this.tableName, fromRecord, true, skipKeyViolators);
					}

				} catch (Exception e) {
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
	 * @param object
	 * @return
	 */
	private int columnCount(String tableName) {
		Query query = new Query(tableName);
		DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
		PreparedQuery preparedQuery = ds.prepare(query);
		Entity entity = preparedQuery.asSingleEntity();

		return entity == null ? 0 : entity.getProperties().keySet().size();
	}

	/**
	 * @param tableName
	 * @return
	 */
	private boolean tableExists(String tableName) {
		Query query = new Query(tableName);
		query.setKeysOnly();

		DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
		PreparedQuery preparedQuery = ds.prepare(query);
		Entity entity = preparedQuery.asSingleEntity();

		return entity != null;
	}

	/**
	 * Populate tableName with data fetched by the parser, first advancing to resumePos.
	 * 
	 * For Full imports, if skipKeyViolators is True, any insertions which would violate the primary key constraint will be skipped and won't log errors.
	 * 
	 * @throws IOException
	 * @throws SubstringNotFoundException
	 */
	private void populateTable(String tableName, long resumeNum/* =0 */, boolean isIncremental/* =False */, boolean skipKeyViolators/* =False */)
			throws IOException, SubstringNotFoundException {

		DatastoreService ds = DatastoreServiceFactory.getDatastoreService();

		this.parser.seekToRecord(resumeNum); // advance to resumeNum

		while (true) {

			// By default, we concatenate 200 inserts into a single INSERT statement.
			// a large batch size per insert improves performance, until you start hitting max_packet_size issues.
			// If you increase MySQL server's max_packet_size, you may get increased performance by increasing maxNum
			List<List<String>> records = this.parser.nextRecords(200);
			if (records == null || records.size() == 0) {
				break;
			}

			List<Entity> entities = new ArrayList<Entity>();

			for (List<String> aRecord : records) {
				boolean newEntity = false;

				Filter filter = null;

				if (this.parser.getPrimaryKey().size() > 1) {
					Collection<Filter> subfilters = new ArrayList<Filter>();

					for (String key : this.parser.getPrimaryKey()) {
						subfilters.add(new FilterPredicate(key, FilterOperator.EQUAL, getValue(key, this.parser.getColumnNames(), aRecord)));
					}

					filter = new CompositeFilter(CompositeFilterOperator.AND, subfilters);
				} else if (this.parser.getPrimaryKey().size() > 0) {
					String key = this.parser.getPrimaryKey().get(0);
					filter = new FilterPredicate(key, FilterOperator.EQUAL, getValue(key, this.parser.getColumnNames(), aRecord));
				}

				Query query = new Query(tableName);
				query.setFilter(filter);

				PreparedQuery preparedQuery = ds.prepare(query);

				Entity entity = preparedQuery.asSingleEntity();

				if (entity == null) {
					entity = new Entity(tableName);
					newEntity = true;
				}

				if (!newEntity && skipKeyViolators && !isIncremental) {

				} else {
					setEntityProperties(entity, this.parser.getPrimaryKey(), this.parser.getColumnNames(), aRecord, newEntity);
					entities.add(entity);
				}
			}

			ds.put(entities);

			this.lastRecordIngested = this.parser.getLatestRecordNum();
			long recCheck = checkProgress(5000, 120 * 1000);

			if (recCheck != 0) {
				if (LOGGER.isInfoEnabled()) {
					LOGGER.info(String.format("...at record %d...", recCheck));
				}
			}
		}
	}

	/**
	 * @param primaryKey
	 * @param aRecord
	 * @return
	 */
	private String getValue(String key, List<String> columnNames, List<String> values) {
		return values.get(columnNames.indexOf(key));
	}

	private void setEntityProperties(Entity entity, List<String> primaryKey, List<String> columnNames, List<String> record, boolean newEntity) {

		for (int i = 0; i < columnNames.size(); i++) {
			String exStr = record.get(i);
			String columnName = columnNames.get(i);

			Object value = null;

			if (!exStr.equalsIgnoreCase("'NULL'")) {
				if (exStr.length() >= 500) {
					value = new Text(exStr);
				} else {
					value = exStr;
				}
			}

			if (primaryKey.contains(columnName)) {
				entity.setProperty(columnName, value);
			} else {
				entity.setUnindexedProperty(columnName, value);
			}
		}

		if (!newEntity) {
			List<String> removeProperties = new ArrayList<String>();
			for (String propertyName : entity.getProperties().keySet()) {
				if (!columnNames.contains(propertyName)) {
					removeProperties.add(propertyName);
				}
			}

			for (String propertyName : removeProperties) {
				entity.removeProperty(propertyName);
			}
		}

	}
}
