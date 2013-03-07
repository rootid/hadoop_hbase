package com.ub.hbase.reducer;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import static com.ub.hbase.constants.HBaseConstants.*;
/**
 * Reducer class
 * @author hduser
 *
 */
public class TestReducer extends TableReducer<Text,IntWritable,
ImmutableBytesWritable>
{


	public void reduce(Text key, Iterable<IntWritable> values, 
			Context context)
	{
			int sum = 0;
			for (IntWritable cnt : values) {
				sum += cnt.get();
			}

			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(WORD_CNT,WORD_CNT,Bytes.toBytes(sum));
			try {
				
				context.write(null, put);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

	@Override
	public void cleanup(Context context) {


	}

}
