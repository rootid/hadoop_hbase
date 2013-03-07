package com.ub.hbase.driver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *	This class is used to create the schema for HTTable.
 *	This class populates column family and columns 
 * @author hduser
 *
 */
public class Schema {

	Map<String,List<String>> tableMap;
	String tableName;

	/**
	 * Default constructor
	 */
	public Schema() {
		tableMap = new HashMap<String, List<String>>();
	}
	/**
	 * 
	 * @param aTableName : Create table with tableName
	 */
	public Schema(String aTableName) {
		tableMap = new HashMap<String, List<String>>();
		this.tableName = aTableName;
	}

	public Schema(String aTableName,Map<String,List<String>> aTableMap) {
		this.tableName = aTableName;
		this.tableMap = aTableMap;
	}



	/**
	 * 
	 * @param columnFamily : name of columnFamily
	 */
	private void addColumnFamily(String columnFamily) {
		if(!tableMap.containsKey(columnFamily)) {
			List<String> localColumnList = new ArrayList<String>();
			tableMap.put(columnFamily, localColumnList);
		}
	}
	/**
	 * 
	 * @param columnFamily : name of columnFamily
	 * @param column : name of column
	 */
	public void addColumns(String columnFamily,String column) {
		addColumnFamily(columnFamily);
		List<String> localColumnList = tableMap.get(columnFamily);
		if(column != null) {
			localColumnList.add(column);
		}
		tableMap.put(columnFamily, localColumnList);
	}

	/**
	 * 
	 * @return
	 */
	public Map<String, List<String>> getTableMap() {
		return tableMap;
	}

	/**
	 * 
	 * @return
	 */
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
