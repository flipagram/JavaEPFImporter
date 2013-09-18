package com.spacehopperstudios.database;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class Connection {

	private String server;
	private String database;
	private java.sql.Connection connection;
	private String username;
	private String password;
	private ResultSet queryResult;
	private Statement statement;
	private long affectedRowCount;
	private long insertedId;
	private boolean isTransactionMode;

	private static final Logger LOGGER = Logger.getLogger(Connection.class);

	public Connection(String server, String database, String username, String password) throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("Create connection with server " + server + ", database: " + database + ", username: " + username + " and password: " + password);
		}

		if (server == null) throw new NullPointerException("server cannot be null");
		if (database == null) throw new NullPointerException("database cannot be null");
		if (username == null) throw new NullPointerException("username cannot be null");
		if (password == null) throw new NullPointerException("password cannot be null");

		this.server = server;
		this.database = database;
		this.username = username;
		this.password = password;

		String databaseDriver = getDatabaseDriverName();
		Class.forName(databaseDriver).newInstance();
	}

	private String getDatabaseDriverName() {
		return "com.mysql.jdbc.Driver";
	}

	public void connect() throws SQLException {
		String url = "jdbc:mysql://" + server + "/" + database;

		if (connection == null) {
			connection = DriverManager.getConnection(url, username, password);
			executeQuery("set names \'utf8\'");
		}
	}

	public void executeQuery(String query) throws NullPointerException, SQLException {
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("executing query: " + query);
		}

		if (query == null) throw new NullPointerException("query cannot be null");
		if (query.length() == 0) throw new IllegalArgumentException("query cannot be empty");

		affectedRowCount = -1;
		statement = null;
		insertedId = -1;

		connect();

		statement = connection.createStatement();
		if (statement.execute(query, Statement.RETURN_GENERATED_KEYS)) {
			queryResult = statement.getResultSet();
		} else {
			queryResult = statement.getGeneratedKeys();
		}
	}

	public long getInsertedId() throws SQLException {
		long insertedId = 0;

		if (this.insertedId != -1) {
			insertedId = this.insertedId;
		} else {
			if (queryResult != null) {

				if (queryResult.next()) {
					insertedId = this.insertedId = queryResult.getInt(1);
				}

			}
		}

		return insertedId;
	}

	public boolean fetchNextRow() throws SQLException {
		boolean fetched = false;
		if (queryResult != null) {
			if (fetched = queryResult.next()) {
				// everyone is happy
			}
		}

		return fetched;
	}

	public Object getCurrentRowValue(String key) throws SQLException {
		Object value = null;

		if (queryResult != null) {
			value = queryResult.getObject(key);
		}

		return value;
	}

	public Integer getCurrentRowInteger(String key) throws SQLException {
		Integer value = null;

		if (queryResult != null) {
			value = queryResult.getInt(key);
		}

		return value;
	}

	public String getCurrentRowString(String key) throws SQLException {
		String value = null;

		if (queryResult != null) {
			value = queryResult.getString(key);
		}

		return value;
	}

	public int getRowCount() throws SQLException {
		int count = 0;

		if (queryResult != null) {
			queryResult.last();
			count = queryResult.getRow();
			queryResult.beforeFirst();
		}

		return count;
	}

	public void disconnect() throws SQLException {
		if (connection != null) {
			if (!connection.isClosed()) {
				connection.close();
				connection = null;
			}
		}
	}

	public long getAffectedRowCount() throws SQLException {
		if (statement != null) {
			affectedRowCount = statement.getUpdateCount();
		}

		return affectedRowCount;
	}

	public boolean isConnected() throws SQLException {
		return connection != null && !connection.isClosed();
	}

	public void commit() throws SQLException {
		if (isTransactionMode) {
			if (isConnected()) {
				connection.commit();
			}
		} else {
			LOGGER.info("Attemting to commit when not in transaction mode");
		}
	}

	public void setTransactionMode(boolean transactional) {
		if (connection == null) {
			if (isTransactionMode != transactional) {
				isTransactionMode = transactional;
			}
		}
	}

}