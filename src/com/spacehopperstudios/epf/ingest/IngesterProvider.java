//  
//  Factory.java
//  epfimporter
//
//  Created by William Shakour on 7 Aug 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package com.spacehopperstudios.epf.ingest;

/**
 * @author billy1380
 *
 */
public class IngesterProvider {
	public static Ingester getNamed(String name) { 
		if ("MySql".equalsIgnoreCase(name)) {
			return new MySqlIngester();
		} else if ("DataStore".equalsIgnoreCase(name)) {
			return new DataStoreIngester();
		}
		
		return null;
	}
}
