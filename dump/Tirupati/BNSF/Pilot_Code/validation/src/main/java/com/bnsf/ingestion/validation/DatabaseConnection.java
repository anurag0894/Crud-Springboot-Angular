package com.bnsf.ingestion.validation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.Exception;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class DatabaseConnection {

	public Statement hiveStmnt = null;
	public Statement tdStmnt = null;
	public Statement db2Stmnt = null;
	public Configuration config = null;
	public ResultSet rsTD = null;
	public ResultSet rsHive = null;

	// Reading properties
	Properties props = new Properties();
	InputStream is = null;

	/**
	 * Default constructor used to load the connection parameters from property file.
	 */
	public DatabaseConnection() throws Exception{

		try {
			// is = this.getClass().getResourceAsStream(Constants.VALIDATE_PROP_FILE);
			// props.load(is);
FileInputStream in = new FileInputStream(Constants.VALIDATE_PROP_FILE);
props.load(in);
in.close();

		} catch (IOException ioe) {
			throw new IOException("Property File "+Constants.VALIDATE_PROP_FILE+" could not be loaded from specified path");
		} catch (Exception exe) {
			throw new Exception("Property File "+Constants.VALIDATE_PROP_FILE+" could not be loaded from specified path");
		}

	}

	/**
	 * Method to close all connection objects instantiated
	 */
	public void close() {
		try {
			System.out.println("Closing all Connection related objects");
			if (rsTD != null)
				rsTD.close();

			if (rsHive != null)
				rsHive.close();

			if (hiveStmnt != null)
				hiveStmnt.close();

			if (tdStmnt != null)
				tdStmnt.close();

			if (db2Stmnt != null)
				db2Stmnt.close();

		} catch (Exception exe) {
			exe.printStackTrace();
		}

	}

	/**
	 * Method to create a Hive Connection
	 * 
	 * @return Connection
	 */
	public Connection connectHive() {

		config = new org.apache.hadoop.conf.Configuration();
		Connection hiveCon = null;
		try {

			config.set(props.getProperty("hive.authentication.key"), props.getProperty("hive.authentication.value"));

			UserGroupInformation.setConfiguration(config);
			UserGroupInformation.loginUserFromKeytab(props.getProperty("hive.key"), props.getProperty("hive.key.path"));

			Class.forName(props.getProperty("hive.conn.driver"));

			hiveCon = DriverManager.getConnection(props.getProperty("hive.conn.url"));
			// hiveStmnt = hiveCon.createStatement();
			System.out.println("Connection to Hive Established");

		} catch (Exception exe) {
			exe.printStackTrace();
		}

		return hiveCon;
	}

	/**
	 * Method to create a connection to Teradata Database
	 * 
	 * @return Connection
	 */
	public Connection connectTD() {

		Connection tdCon = null;
		try {

			Class.forName(props.getProperty("td.conn.driver"));
			String url = props.getProperty("td.conn.url");
			String username = props.getProperty("td.dbuser");
			String password = props.getProperty("td.dbpassword");

			tdCon = DriverManager.getConnection(url, username, password);
			// tdStmnt = tdCon.createStatement();
			System.out.println("Connection to Teradata established");

		} catch (Exception exe) {
			exe.printStackTrace();
		}
		return tdCon;
	}

	/**
	 * Method to create a connection to DB2 Database
	 * 
	 * @return Connection
	 */
	public Connection connectDB2() {

		Connection db2Con = null;
		try {

			Class.forName(props.getProperty("db2.conn.driver"));
			String url = props.getProperty("db2.conn.url");
			String username = props.getProperty("db2.dbuser");
			String password = props.getProperty("db2.dbpassword");
			db2Con = DriverManager.getConnection(url, username, password);
			// db2Stmnt = db2Con.createStatement();
			System.out.println("Connection to DB2 has been established.");

		} catch (Exception exe) {
			exe.printStackTrace();
		}
		return db2Con;
	}

}
