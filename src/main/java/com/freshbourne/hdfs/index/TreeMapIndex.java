package com.freshbourne.hdfs.index;

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

public abstract class TreeMapIndex extends TreeMap<String,Long> implements Serializable, Index<Long> {
	private static final long serialVersionUID = 1L;
	protected static final Log LOG = LogFactory.getLog(TreeMapIndex.class);
	
	// not static final, so that it can be inherited and overwritten by a configuration
	private String savePath = "/tmp/RENAME_INDEX";
	protected Select select;
	
	protected long highestOffset = -1;
	
	public TreeMapIndex(){super();load(savePath);}
	
	public TreeMapIndex load(String path) {
		TreeMapIndex result = null;
		try{
        FileInputStream in = new FileInputStream(path); 
        ObjectInputStream s = new ObjectInputStream(in); 
        result = (TreeMapIndex) s.readObject();
        LOG.info("INDEX LOADED: " + result.toString());
		} catch(Exception e){
			LOG.warn("Could not load the index: " + e.getMessage());
		}
        return result;
	}
	
	public void save() throws FileNotFoundException{
		if(savePath == null)
			throw new FileNotFoundException("you must provide a savePath for this Index");
		save(savePath);
	}
	
	@Override
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
	

	
	
	@Override
	public abstract void add(String[] splits, Long offset);
	public long getHighestOffset(){return highestOffset;}
	
	@Override
	public EntryIterator getIterator(){
		EntryIterator ei = new EntryIterator(entrySet().iterator(), getHighestOffset());
		ei.setSelect(select);
		return ei;
	}
	
	public void setSavePath(String savePath) {
		this.savePath = savePath;
	}

	public String getSavePath() {
		return savePath;
	}
	
	public void setSelect(Select s){
		select = s;
	}

	public class EntryIterator implements Iterator<Map.Entry<String, Long>>{
		private Iterator<Map.Entry<String, Long>> i;
		private Select select;
		private Entry<String,Long> entry;
		
		private final long highOffset;
		
		public EntryIterator(Iterator<Map.Entry<String, Long>> i, long offset){
			super();
			this.i = i;
			this.highOffset = offset;
		}
		
		public long getHighestOffset(){return highOffset;}

		@Override
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

		@Override
		public Entry<String, Long> next() {
			LOG.info("value of entry: " + entry.getKey());
			Entry<String,Long> e = entry;
			entry = null;
			return e;
		}

		@Override
		public void remove() {
			//TODO: bug here, since we moved iterator
			i.remove();
		}
		
	}
}
