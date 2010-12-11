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

public abstract class Index extends TreeMap<String,Long> implements Serializable {
	private static final long serialVersionUID = 1L;
	protected static final Log LOG = LogFactory.getLog(Index.class);
	
	// not static final, so that it can be inherited and overwritten by a configuration
	private String savePath = "/tmp/RENAME_INDEX";
	protected Select select;
	
	protected long highestOffset = -1;
	
	public Index(){super();}
	
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
	

	
	
	public abstract void add(String[] splits, long offset);
	public long getHighestOffset(){return highestOffset;}
	
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
