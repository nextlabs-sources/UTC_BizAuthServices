package com.nextlabs.ac.helper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.bluejungle.framework.crypt.IDecryptor;
import com.bluejungle.framework.crypt.ReversibleEncryptor;

public class ConnectionManager {
	private static final Log LOG = LogFactory.getLog(ConnectionManager.class);
	static ConnectionManager connectionPool = null;
	Queue<Connection> connectionList;
	int usedConnection;
	String connectionUrl;
	String userName;
	String password;
	int connectionPoolSize;
	boolean isInitialized = false;
	private IDecryptor decryptor = new ReversibleEncryptor();
	private ConnectionManager(String connectionUrl, String userName,
			String password, int connectionPoolSize) {
		this.connectionUrl = connectionUrl;
		this.userName = userName;
		this.password =decryptor.decrypt(password);
		this.connectionPoolSize = connectionPoolSize;
		connectionList = new LinkedList<Connection>();
		usedConnection = 0;
	}

	public static ConnectionManager getInstance(String connectionUrl,
			String userName, String password, int connectionPoolSize) {
		if (connectionPool == null) {
			connectionPool = new ConnectionManager(connectionUrl, userName,
					password, connectionPoolSize);
			connectionPool.initializeConnection();
		}

		return connectionPool;

	}

	public void initializeConnection() {

		while (isPoolFree()) {
			Connection conn = refillConnection();
			if (conn != null)
				connectionList.add(conn);
			else
				break;
		}

	}

	private boolean isPoolFree() {

		if ((usedConnection + connectionList.size()) < connectionPoolSize)
			return true;
		return false;
	}

	private Connection refillConnection() {
		Connection hsqlConnection = null;
		try {
			hsqlConnection = DriverManager.getConnection(connectionUrl,
					userName, password);
		} catch (SQLException e) {
			LOG.error("ConnectionPool initializeConnection() error: ", e);
		}
		return hsqlConnection;
	}

	public synchronized Connection getConnection() {
		if (connectionList.size() > 0) {
			Connection hsqlConnection = connectionList.remove();
			usedConnection++;
			try {
				if (!hsqlConnection.isClosed())
					return hsqlConnection;
				else {
					hsqlConnection = refillConnection();
					if (hsqlConnection != null) {
						return hsqlConnection;
					}
				}
			} catch (SQLException e) {
				LOG.error("ConnectionPool getConnection() error: ", e);
			}

		} else if (isPoolFree()) {
			Connection conn = refillConnection();
			if (conn != null) {
				usedConnection++;
				return conn;
			}
		}
		return null;
	}

	public synchronized void handoverConnection(Connection connection) {
		usedConnection = usedConnection - 1;
		try {
			if (!connection.isClosed()) {
				connection.clearWarnings();
				connectionList.add(connection);
			} else {
				if (isPoolFree()) {
					Connection conn = refillConnection();
					if (conn != null) {
						connectionList.add(conn);
					}
				}
			}
		} catch (SQLException e) {
			LOG.error("ConnectionPool HandoverConnection() error: ", e);
		}
	}

}
