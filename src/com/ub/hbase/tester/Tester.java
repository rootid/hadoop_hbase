package com.ub.hbase.tester;

import static com.ub.hbase.constants.HBaseConstants.TEST_TABLE;
import static com.ub.hbase.constants.HBaseConstants.WORD_CNT_STR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import com.ub.hbase.driver.HbaseJobConfig;
import com.ub.hbase.driver.Schema;
import com.ub.hbase.mapper.TestMapper;
import com.ub.hbase.reducer.TestReducer;
import com.ub.hbase.util.HbaseUtils;


public class Tester {

	public static void main(String args[]){
		
		Schema optable;
		optable = new Schema(TEST_TABLE);
		optable.addColumns(WORD_CNT_STR, null);
		
		Configuration hbaseConfig = HbaseUtils.configureHbase();
		
		try {
			HbaseUtils.makeTableIfNeeded(optable,hbaseConfig);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		//create simple M-R job
		Configuration conf = new Configuration();
		
		HbaseJobConfig jobConfig = new HbaseJobConfig(hbaseConfig, 
				"Test M-R Job");
		jobConfig.configureJob(Tester.class, 1);
		
		Scan scan = null;
		scan = new Scan();
		
//		jobConfig.initMapper("", scan, TestMapper.class, 
//				IntWritable.class,Text.class);
		
		jobConfig.setMapper(TestMapper.class,
				Text.class,IntWritable.class);
		
		jobConfig.initTableReducer(TEST_TABLE, null, TestReducer.class);
		
		try {
			
			FileInputFormat.addInputPath(jobConfig.getMrJob(),
					new Path(args[0]));
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		 try {
			boolean status = jobConfig.getMrJob().waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
}
