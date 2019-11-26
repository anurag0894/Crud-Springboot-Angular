package com.bnsf.ingestion.validation;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.owasp.esapi.ESAPI;
import org.owasp.esapi.errors.IntrusionException;
import org.owasp.esapi.errors.ValidationException;
import java.io.IOException;
import java.lang.Exception;
import java.math.BigDecimal;

import com.bnsf.fr.util.Encoders;

public class ValidationBase {

	public static String tableName = Constants.BLANK_STRING;
	public static String loadType = Constants.BLANK_STRING;
	public static String srcType = Constants.BLANK_STRING;
	public static String maxColVal = Constants.BLANK_STRING;
	public static String targetTS = Constants.BLANK_STRING;
	public static String startdate = Constants.BLANK_STRING;
	public static String enddate = Constants.BLANK_STRING;
	public static String checkColumn = Constants.BLANK_STRING;

	public Double tdAvg = 0.0;
	public Double hvAvg = 0.0;
	
	public static PreparedStatement hiveStmnt = null;
	public static PreparedStatement tdStmnt = null;
	public static PreparedStatement db2Stmnt = null;
	public static Connection hiveCon = null;
	public static Connection tdCon = null;
	public static Connection db2Con = null;
	public ResultSet rsTD = null;
	public ResultSet rsHive = null;
	public Double delta = 0.0;

	public String tableAlias = "msg.";
	public static DatabaseConnection conn = null;

	/**
	 * Static method used to set up the one time database connection to
	 * Teradata,DB2 and Hive.
	 */
	@BeforeClass
	public static void setUpConnection() throws Exception {

		conn = new DatabaseConnection();
		tdCon = conn.connectTD();
		hiveCon = conn.connectHive();
		db2Con = conn.connectDB2();
	}

	/**
	 * Method to validate the input arguments passed if it adheres to
	 * organization standard
	 * 
	 * @param description
	 * @param item
	 * @param regex
	 * @param maxLength
	 * @param allowNull
	 * @return String
	 * @throws ValidationException
	 */
	private String checkFormat(String description, String item, String regex, int maxLength, boolean allowNull)
			throws ValidationException {

		try {

			return ESAPI.validator().getValidInput(description, item, regex, maxLength, allowNull);

		} catch (ValidationException e) {
			throw new ValidationException("Not Validated", description + item);

		} catch (IntrusionException e) {
			throw new IntrusionException("Intrusion Error on", description + item);
		}

	}

	/**
	 * Test Method to Validate Data Ingested from Source table and Hive.
	 */
	@Test
	public void validateRecord() throws Exception {

		Properties props = new Properties();
		InputStream in = null;
		try {
			in = this.getClass().getResourceAsStream(Constants.QUERY_CONFIG_PROP_FILE);
			props.load(in);
		} catch (IOException ioe) {
			throw new IOException("Property File "+Constants.QUERY_CONFIG_PROP_FILE+" could not be loaded from specified path");
		} catch (Exception ex) {
			throw new Exception("Property File "+Constants.QUERY_CONFIG_PROP_FILE+" could not be loaded from specified path");
		}

		System.out.println("======================================================================");
		System.out.println("Validating Record in Source and Hive DB for table: " + tableName);
		System.out.println("======================================================================");

		String sqlQuery = Constants.BLANK_STRING;
		String hiveSql = Constants.BLANK_STRING;
		String hiveQueryProp = Constants.BLANK_STRING;
		String sqlQueryProp = Constants.BLANK_STRING;
		String sqlQueryIncrProp = Constants.BLANK_STRING;

		System.out.println("-------------------------BUILD QUERY--------------------------");
		System.out.println("Start building query for Source and Hive DB for table: " + tableName);

		tableName = checkFormat("Table", tableName, "TableNames", 30, false);

		hiveQueryProp = tableName.toLowerCase() + Constants.HIVE_PROP_KEY;
		sqlQueryProp = tableName.toLowerCase() + Constants.SQL_PROP_KEY;
		sqlQueryIncrProp = tableName.toLowerCase() + Constants.SQL_PROP_KEY_INCR;

		hiveSql = props.getProperty(hiveQueryProp) + Constants.NEW_TABLE;

		checkColumn = checkFormat("Column", checkColumn, "ColumnNames", 30, true);

		if (StringUtils.isNotEmpty(startdate) && StringUtils.isNotEmpty(enddate)) {

			if (tableName.equalsIgnoreCase(Constants.TABLE_VCAR_REPAIR)
					|| tableName.equalsIgnoreCase(Constants.TABLE_VCAR_REPAIR_DETAIL)) {

				startdate = checkFormat("Date", startdate, "Date", 20, true);
				enddate = checkFormat("Date", enddate, "Date", 20, true);
			} else {

				startdate = checkFormat("Date-Time", startdate, "DateTime", 30, true);
				enddate = checkFormat("Date-Time", enddate, "DateTime", 30, true);

			}
		}

		try {
			if (loadType.equals(Constants.LOAD_TYPE_INITIAL)) {

				if (StringUtils.isNotEmpty(startdate) && StringUtils.isNotEmpty(enddate)) {

					if (tableName.equalsIgnoreCase(Constants.TABLE_EQCMPNT_MEAS)) {
						sqlQuery = props.getProperty(sqlQueryProp) + Constants.WHERE + tableAlias + checkColumn
								+ Constants.GREATER_THAN_EQUAL + Constants.QUESTION_MARK + Constants.AND + tableAlias
								+ checkColumn + Constants.LESS_THAN_EQUAL + Constants.QUESTION_MARK;
					} else {
						sqlQuery = props.getProperty(sqlQueryProp) + Constants.WHERE + checkColumn
								+ Constants.GREATER_THAN_EQUAL + Constants.QUESTION_MARK + Constants.AND + checkColumn
								+ Constants.LESS_THAN_EQUAL + Constants.QUESTION_MARK;
					}

					if (srcType.equals(Constants.SOURCE_TYPE_TERADATA)) {
						tdStmnt = tdCon.prepareStatement(Encoders.encodeForSQL(sqlQuery));
						tdStmnt.setString(1, startdate);
						tdStmnt.setString(2, enddate);
						rsTD = tdStmnt.executeQuery();
					} else {
						sqlQuery = sqlQuery + Constants.WITH_UR;
						db2Stmnt = db2Con.prepareStatement(Encoders.encodeForSQL(sqlQuery));
						db2Stmnt.setString(1, startdate);
						db2Stmnt.setString(2, enddate);
						rsTD = db2Stmnt.executeQuery();
					}
				} else {
					sqlQuery = props.getProperty(sqlQueryProp);
					if (srcType.equals(Constants.SOURCE_TYPE_TERADATA)) {
						tdStmnt = tdCon.prepareStatement(Encoders.encodeForSQL(sqlQuery));
						rsTD = tdStmnt.executeQuery();
					} else {
						db2Stmnt = db2Con.prepareStatement(Encoders.encodeForSQL(sqlQuery));
						rsTD = db2Stmnt.executeQuery();
					}
				}
			}

			else {
				hiveSql = props.getProperty(hiveQueryProp) + Constants.NEW_TABLE;
				if (srcType.equals(Constants.SOURCE_TYPE_TERADATA)) {
					if (tableName.equalsIgnoreCase(Constants.TABLE_VME_CRB_JOB_CODES)){
						checkColumn = Constants.CHECK_COLUMN_JOB_CODE;
					}
					sqlQuery = props.getProperty(sqlQueryProp) + Constants.WHERE + checkColumn + Constants.GREATER_THAN_EQUAL
							+ Constants.QUESTION_MARK + Constants.AND + checkColumn + Constants.LESS_THAN_EQUAL
							+ Constants.QUESTION_MARK;
					tdStmnt = tdCon.prepareStatement(Encoders.encodeForSQL(sqlQuery));
					tdStmnt.setString(1, maxColVal);
					tdStmnt.setString(2, targetTS);
					rsTD = tdStmnt.executeQuery();
				} else {

					if (tableName.equalsIgnoreCase(Constants.TABLE_EQCMPNT_STTC)
							|| tableName.equalsIgnoreCase(Constants.TABLE_EQCMPNT_MEAS)
							|| tableName.equalsIgnoreCase(Constants.TABLE_EQP_STTC)) {
						sqlQuery = props.getProperty(sqlQueryIncrProp) + Constants.WHERE + tableAlias + checkColumn
								+ Constants.GREATER_THAN + Constants.QUESTION_MARK + Constants.AND + tableAlias
								+ checkColumn + Constants.LESS_THAN_EQUAL + Constants.QUESTION_MARK + Constants.WITH_UR;
					} else {
						sqlQuery = props.getProperty(sqlQueryIncrProp) + Constants.WHERE + checkColumn
								+ Constants.GREATER_THAN + Constants.QUESTION_MARK + Constants.AND + checkColumn
								+ Constants.LESS_THAN_EQUAL + Constants.QUESTION_MARK + Constants.WITH_UR;
					}
					db2Stmnt = db2Con.prepareStatement(Encoders.encodeForSQL(sqlQuery));
					db2Stmnt.setString(1, maxColVal);
					db2Stmnt.setString(2, targetTS);
					rsTD = db2Stmnt.executeQuery();
				}

			}

		} catch (Exception ex) {
			ex.printStackTrace();
			throw new Exception("Error executing Query for Validation Test Method");
		}

		try {
			System.out.println("--------------------------EXEC QUERY--------------------------");
			System.out.println("Source Table Query: " + sqlQuery);
			
			System.out.println("Hive Table Query: " + hiveSql);
			hiveStmnt = hiveCon.prepareStatement(hiveSql);
			rsHive = hiveStmnt.executeQuery(Encoders.encodeForSQL(hiveSql));
			
			System.out.println("======================START TEST CASES EXECUTION=======================");
			if (rsHive.next() && rsTD.next()) {
				System.out.println("Hive table query executed successfully");
				ResultSetMetaData rsMD = rsHive.getMetaData();
				
				
				for(int index=1;index<=rsMD.getColumnCount();index++){				
					compareData(rsTD,rsHive, rsMD, index);			
				}
				
			}
			
			System.out.println("======================END TEST CASES EXECUTION========================");
		 } catch (Exception exe) {
			 exe.printStackTrace();
			throw new Exception("ResultSet not initialized or empty");
		 }

		
		System.out.println("======================================================================");
		System.out.println("Record validated successfully for table: " + tableName);
		System.out.println("======================================================================");

	}
	
	/**
	 * Method to compare the source data and hive data
	 */
	private void compareData(ResultSet rsTd,ResultSet rsHive,ResultSetMetaData rsMD, int index) throws Exception{
		
		String columnType = rsMD.getColumnTypeName(index);
		String columnAlias = rsMD.getColumnLabel(index).toLowerCase();
		
		System.out.println("Column Type is: " + columnType);
		
		switch(columnType){
		case "bigint" :
			
			System.out.println("Source value: " + rsTD.getLong(index));
			System.out.println("Hive value: " + rsHive.getLong(index));
			assertEquals(rsTD.getLong(index), rsHive.getLong(index));
			break;
		case "int" :
			
			System.out.println("Source value: " + rsTD.getInt(index));
			System.out.println("Hive value: " + rsHive.getInt(index));
			assertEquals(rsTD.getInt(index), rsHive.getInt(index));
			break;
		case "short" :
			
			System.out.println("Source value: " + rsTD.getShort(index));
			System.out.println("Hive value: " + rsHive.getShort(index));
			assertEquals(rsTD.getShort(index), rsHive.getShort(index));
			break;
		case "timestamp" :
			
			System.out.println("Source value: " + rsTD.getTimestamp(index));
			System.out.println("Hive value: " + rsHive.getTimestamp(index));
			assertEquals(rsTD.getTimestamp(index), rsHive.getTimestamp(index));
			break;
		case "date" :
			
			System.out.println("Source value: " + rsTD.getDate(index));
			System.out.println("Hive value: " + rsHive.getDate(index));
			assertEquals(rsTD.getDate(index), rsHive.getDate(index));
			break;
		case "string" :
			
			System.out.println("Source value: " + rsTD.getString(index));
			System.out.println("Hive value: " + rsHive.getString(index));
			assertEquals(rsTD.getString(index), rsHive.getString(index));
			break;
		case "double" :
			
			tdAvg = Math.floor(rsTD.getDouble(index) * 10.0) / 10.0;
			delta = Math.abs(tdAvg) / 1e3;
			hvAvg = Math.floor(rsHive.getDouble(index) * 10.0) / 10.0;
			System.out.println("Source value: " + tdAvg);
			System.out.println("Hive value: " + hvAvg);
			assertEquals(tdAvg, hvAvg, delta);
			break;
		case "decimal" :
			BigDecimal bg1 = new BigDecimal(rsTD.getBigDecimal(index).toString());
			BigDecimal bg2 = new BigDecimal(rsHive.getBigDecimal(index).toString());
			BigDecimal resultTD = bg1.setScale(2,BigDecimal.ROUND_UP);
			BigDecimal resultHive = bg2.setScale(2,BigDecimal.ROUND_UP);
			System.out.println("Source value: " + resultTD);
			System.out.println("Hive value: " + resultHive);
			assertEquals(resultTD, resultHive);
			break;
			
		}
		
	}

	/**
	 * Main method where the program execution starts
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		try {
			tableName = args[0];
			loadType = args[1];
			srcType = args[2];
			if (StringUtils.isNotEmpty(args[3])) {

				maxColVal = args[3];
				startdate = args[3];

			}
			if (StringUtils.isNotEmpty(args[4])) {

				targetTS = args[4];
				enddate = args[4];

			}
			if (StringUtils.isNotEmpty(args[5])) {
				checkColumn = args[5];
			}
			JUnitCore.main(ValidationBase.class.getName());
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Static method to close the database connection created.
	 * 
	 */
	@AfterClass
	public static void afterClass() {
		conn.close();
	}

}
