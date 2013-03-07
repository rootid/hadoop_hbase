package com.ub.hbase.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Mapper class
 * @author hduser
 *
 */
public class TestMapper extends TableMapper<Text,IntWritable> {

	@Override
	public void map(ImmutableBytesWritable row, Result value,
			Context context)  {
		Text word = new Text();
		 
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			try {
				context.write(word,new IntWritable(1));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}
	
	
	@Override
	public void cleanup(Context context) {
		
		
	}
	
	
}
