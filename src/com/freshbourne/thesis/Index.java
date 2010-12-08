package com.freshbourne.thesis;

import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public abstract class Index {
	private TreeMap<String, Long> tree = new TreeMap<String, Long>();
	private static final Log LOG = LogFactory.getLog(LineRecordReader.class);

	
	protected int COLUMN;
	
	public void add(String[] splits, long offset){
		if(splits.length > COLUMN){
			tree.put(splits[COLUMN], offset);
		}
		LOG.info(tree.toString());
	}
}
