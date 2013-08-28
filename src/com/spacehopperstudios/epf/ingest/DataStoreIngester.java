//  
//  DataStoreIngester.java
//  epfimporter
//
//  Created by William Shakour on 7 Aug 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package com.spacehopperstudios.epf.ingest;

import org.apache.log4j.Logger;

import com.spacehopperstudios.epf.parse.V3Parser;

/**
 * @author billy1380
 * 
 */
public class DataStoreIngester extends IngesterBase implements Ingester {

	private static final Logger LOGGER = Logger.getLogger(DataStoreIngester.class);

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

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Init ended");
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.spacehopperstudios.epf.ingest.Ingester#ingest(boolean)
	 */
	@Override
	public void ingest(boolean skipViolators) {

		System.out.println("Ingest not implemented in " + getClass().getName());

		throw new UnsupportedOperationException();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.spacehopperstudios.epf.ingest.Ingester#ingestFull(boolean)
	 */
	@Override
	public void ingestFull(boolean skipKeyViolators) {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.spacehopperstudios.epf.ingest.Ingester#ingestFullResume(long, boolean)
	 */
	@Override
	public void ingestFullResume(long fromRecord, boolean skipKeyViolators) {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.spacehopperstudios.epf.ingest.Ingester#ingestIncremental(long, boolean)
	 */
	@Override
	public void ingestIncremental(long fromRecord, boolean skipKeyViolators) {
		// TODO Auto-generated method stub

	}

}
