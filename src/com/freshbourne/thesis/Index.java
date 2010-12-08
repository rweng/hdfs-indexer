package com.freshbourne.thesis;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class Index {
	private TreeMap<String, Long> tree = new TreeMap<String, Long>();
	private static final Log LOG = LogFactory.getLog(LineRecordReader.class);
	
	protected int COLUMN;
	private long highestOffset = 0;
	
	public Index(int col){
		COLUMN = col;
	}
	
	public int getColumn(){return COLUMN;}
	
	public void add(String[] splits, long offset){
		if(offset <= highestOffset)
			return;
		
		if(splits.length > COLUMN){
			highestOffset = offset;
			tree.put(splits[COLUMN], offset);
		}
		LOG.info(tree.toString());
	}
	
	public long getHighestOffset(){return highestOffset;}
	
	public EntryIterator getIterator(){
		return new EntryIterator(tree.entrySet().iterator(), getHighestOffset());
	}
	
	public class EntryIterator implements Iterator<Map.Entry<String, Long>>{
		private Iterator<Map.Entry<String, Long>> i;
		private Select select;
		private Entry<String,Long> entry;
		private long highOffset = 0;
		
		public EntryIterator(Iterator<Map.Entry<String, Long>> i, long offset){
			super();
			this.i = i;
			highestOffset = offset;
		}
		
		public long getHighestOffset(){return highestOffset;}

		public boolean hasNext() {
			if(select == null)
				return i.hasNext();
			
			if(entry != null)
				return true;
			
			while(i.hasNext()){
				entry = i.next();
				if(select.select(entry.getKey()))
					return true;
			}
			
			entry = null;
			return false;
		}
		
		public void setSelect(Select s){select = s;}

		public Entry<String, Long> next() {
			Entry<String,Long> e = entry;
			entry = null;
			return e;
		}

		public void remove() {
			//TODO: bug here, since we moved iterator
			i.remove();
		}
		
	}
}
