package com.freshbourne.thesis;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class Index extends TreeMap<String,Long> implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(LineRecordReader.class);
	private String savePath;
	
	protected int COLUMN;
	private long highestOffset = -1;
	
	public Index(int col){
		COLUMN = col;
	}
	
	public static Index load(String path) throws IOException, ClassNotFoundException{
        FileInputStream in = new FileInputStream(path); 
        ObjectInputStream s = new ObjectInputStream(in); 
        Index result = (Index) s.readObject();
        LOG.info("INDEX LOADED: " + result.toString());
        return result;
	}
	
	public void save() throws FileNotFoundException{
		if(savePath == null)
			throw new FileNotFoundException("you must provide a savePath for this Index");
		save(savePath);
	}
	
	public void save(String filename) {
		FileOutputStream fos = null;
		ObjectOutputStream out = null;
		try {
			fos = new FileOutputStream(filename);
			out = new ObjectOutputStream(fos);
			out.writeObject(this);
			out.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	public int getColumn(){return COLUMN;}
	
	public void add(String[] splits, long offset){
		if(offset <= highestOffset)
			return;
		
		if(splits.length > COLUMN){
			highestOffset = offset;
			put(splits[COLUMN], offset);
		}
		LOG.info(toString());
	}
	
	public long getHighestOffset(){return highestOffset;}
	
	public EntryIterator getIterator(){
		return new EntryIterator(entrySet().iterator(), getHighestOffset());
	}
	
	public void setSavePath(String savePath) {
		this.savePath = savePath;
	}

	public String getSavePath() {
		return savePath;
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
			if(select == null){
				if(!i.hasNext())
					return false;
				
				entry = i.next();
				return true;
			}
			if(entry != null)
				return true;
			
			while(i.hasNext()){
				entry = i.next();
				if(select.select(entry.getKey())){
					LOG.info("GOT THE KEY");
					return true;
				}
			}
			
			entry = null;
			return false;
		}
		
		public void setSelect(Select s){select = s;}

		public Entry<String, Long> next() {
			LOG.info("value of entry: " + entry.getKey());
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
