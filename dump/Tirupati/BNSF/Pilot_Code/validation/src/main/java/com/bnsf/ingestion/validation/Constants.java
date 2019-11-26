package com.bnsf.ingestion.validation;

public class Constants {

	public static final String BLANK_STRING = "";
	public static final String AND = " and ";
	public static final String SOURCE_TYPE_TERADATA = "teradata";
	public static final String CURRENT_TABLE = "_CURRENT";
	public static final String NEW_TABLE = "_NEW";
	public static final String GREATER_THAN = " > ";
	public static final String LESS_THAN_EQUAL = " <= ";
	public static final String GREATER_THAN_EQUAL = " >= ";
	public static final String WITH_UR = " with ur";
	public static final String QUERY_CONFIG_PROP_FILE = "/config/QueryConfig.properties";
	public static final String LOAD_TYPE_INITIAL = "INITIAL";
	public static final String HIVE_PROP_KEY = ".hive.query";
	public static final String SQL_PROP_KEY = ".sql.query";
	public static final String SQL_PROP_KEY_INCR = ".sql.query.incr";
	public static final String TABLE_VCAR_REPAIR = "vcar_repair";
	public static final String TABLE_VCAR_REPAIR_DETAIL = "vcar_repair_detail";
	public static final String TABLE_VME_CRB_JOB_CODES = "vme_crb_job_codes";
	public static final String TABLE_VMEDA_DA_DETR_RGSTRY = "vmeda_da_detr_rgstry";
	public static final String TABLE_TEDR_CO = "tedr_co";
	public static final String TABLE_EQCMPNT_STTC = "VMEDA_DA_EQCMPNT_STTC";
	public static final String TABLE_EQCMPNT_MEAS = "VMEDA_DA_EQCMPNT_MEAS";
	public static final String TABLE_EQP_STTC = "VMEDA_DA_EQP_STTC";
	public static final String DELIMETER_SINGLE_QUOTE = "'";
	public static final String WHERE = " where ";
	public static final String QUESTION_MARK = " ? ";
	public static final String DATE_FORMAT = "yyyy-MM-dd";
	public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    //public static final String VALIDATE_PROP_FILE	= "/config/data-validation.properties";
     public static final String VALIDATE_PROP_FILE       = System.getProperty("cfg.prop");

	public static final String DATETIME = ".DateTime";
	public static final String TABLE_NAME = ".TableNames";
	public static final String COLUMN_NAME = ".ColumnNames";
	
	public static final String CHECK_COLUMN_JOB_CODE = "LST_UPDT_TMSTP";

}

