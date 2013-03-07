package com.ub.hbase.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import com.ub.hbase.driver.Schema;
/**
 * 
 * @author hduser
 *
 */
public final class HbaseUtils {

	private static Configuration hbaseConfig;
	
	/**
	 * 
	 * @param conf
	 * @param table 
	 * @return
	 * @throws IOException
	 */
	public static HTable makeTableIfNeeded(Schema table,Configuration conf) 
			throws IOException {
//		Configuration conf = HBaseConfiguration.create();
		String tableName = table.getTableName();
		Map<String,List<String>> tableMap = table.getTableMap();
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor descriptor = new HTableDescriptor(tableName);

		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("Deleting existing Table: " + tableName + "\n");
		}
		//TODO : add columns in table for respective family
		if (!admin.tableExists(tableName)) {
			Set<String> columnFamilySet =  tableMap.keySet();
			for(String columnFamily : columnFamilySet) {
				descriptor.addFamily(new HColumnDescriptor(columnFamily));
			}
			admin.createTable(descriptor);
		}

		return new HTable(conf, tableName);
	}
	
	
	public static Configuration configureHbase() {
		// conf.addResource(new Path("../conf/hbase-site.xml"));
		
		hbaseConfig = HBaseConfiguration.create();
		hbaseConfig.set("hbase.zookeeper.quorum", "localhost:2181");
		hbaseConfig.set("mapred.job.tracker", "local");
		hbaseConfig.set("mapred.local.dir", "target/mapreduce/local");
		
		return hbaseConfig;
	}
	
	
}
