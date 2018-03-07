package com.tangqiang;

import org.apache.hadoop.hbase.TableName;

public class TestConfig {
	
	public static String quorom = "10.1.3.51,10.1.3.52";
//	public static String quorom = "master,slave1";
	public static String clientPort = "2181";
	public static TableName tableName = TableName.valueOf("people");
}
