package com.ub.hbase.constants;

import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConstants {

	public static final String TEST_TABLE = "test-table";
	public static final String WORD_STR = "word";
	public static final String WORD_CNT_STR = "word-cnt";
	public static final byte[] WORD  = Bytes.toBytes("word");
	public static final byte[] WORD_CNT  = Bytes.toBytes("word-cnt");
	
}
