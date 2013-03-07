package com.ub.hbase.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;
import com.ub.hbase.mapper.TestMapper;


/**
 * Configure M-R job
 * @author hduser
 *
 */
public class HbaseJobConfig {

	private Job mrJob;
	//private Scan scan;
	
   /**
    * Configure M-R job
    * @param config
    * @param jobName
    */
	public HbaseJobConfig(Configuration config,String jobName) {

		try {
			mrJob = new Job(config,jobName);
	//		scan = new Scan();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	
	/**
	 * Configure job
	 * @param jarClass
	 * @param mrNoTask
	 */
	public void configureJob(Class<?> jarClass, int mrNoTask) {

		mrJob.setJarByClass(jarClass);
		mrJob.setNumReduceTasks(mrNoTask);
	}
	

	/**
	 * Initialize mapper
	 * @param job
	 * @param inputTableName
	 * @param scan
	 * @param mapperClass
	 * @param outputKeyClass
	 * @param outputValueClass
	 */
	
	public void initMapper(String inputTableName,
			Scan scan,
			Class<? extends TableMapper> mapperClass,
			Class<? extends WritableComparable> outputKeyClass, 
            Class<? extends Writable> outputValueClass) {
		
		try {
			mrJob.setMapperClass(mapperClass);
			mrJob.setMapOutputKeyClass(outputKeyClass);
			mrJob.setMapOutputValueClass(outputValueClass);	
			TableMapReduceUtil.initTableMapperJob(
					inputTableName, scan,
					mapperClass, 
					outputKeyClass,outputValueClass,mrJob,false);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Initialize Reducer
	 * @param job
	 * @param outputTableName
	 * @param scan
	 * @param reducerClass
	 */
	public void initReducer(String outputTableName,Scan scan,
			Class<? extends TableReducer> reducerClass) {
		
		mrJob.setReducerClass(reducerClass);
		
		try {
			TableMapReduceUtil.initTableReducerJob(outputTableName,
					reducerClass, mrJob,
					null,null,null,
					null,false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	

	public Job getMrJob() {
		return mrJob;
	}


//	public Scan getScan() {
//		return scan;
//	}
//
//
//	public void setScan(Scan scan) {
//		this.scan = scan;
//	}


	public void setMrJob(Job mrJob) {
		this.mrJob = mrJob;
	}


	public void initMapper(Object inputTableName, Object scan,
			Class<TestMapper> mapperClass, Class<Text> class1,
			Class<DoubleWritable> outputValueClass) {
		// TODO Auto-generated method stub
		
	}

	

}
