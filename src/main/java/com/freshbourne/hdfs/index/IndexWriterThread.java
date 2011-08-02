/**
 * Copyright (C) 2011 Robin Wenglewski <robin@wenglewski.de>
 *
 * This work is licensed under a Creative Commons Attribution-NonCommercial 3.0 Unported License:
 * http://creativecommons.org/licenses/by-nc/3.0/
 * For alternative conditions contact the author. 
 */
package com.freshbourne.hdfs.index;

import java.io.FileOutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.freshbourne.hdfs.index.IndexedRecordReader.Shared;
import com.freshbourne.hdfs.index.IndexedRecordReader.KeyValue;


/**
 * receives a shared object from the IndexRecordReader to handle everything, the IndexRecordReader doesn't want to do:
 * sorting the Array and writing the tree to disk.
 * 
 */
class IndexWriterThread extends Thread {
	
	private static final Log LOG = LogFactory.getLog(IndexWriterThread.class);
	private Shared shared;

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		
		LOG.debug("Running thread");

		shared.getKeyValueList();
		
		try {
			KeyValue sv = shared.getKeyValueList().poll(1, TimeUnit.MINUTES);
			while (!shared.isFinished() && sv != null) {
				shared.getIndex().add(sv.getSplits(), sv.getValue());
				sv = shared.getKeyValueList().poll(1, TimeUnit.MINUTES);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		shared.save();
		LOG.debug("Ending Thread");
	}
	
	public IndexWriterThread(Shared s) {
		this.shared = s;
	}

	synchronized public void save(){
			LOG.debug("saving index");
			index.save();
			LOG.debug("saving properties");
			String[] indexPathSplit = index.getPath().split("/");
			String indexPath = indexPathSplit[indexPathSplit.length - 1 ];
			properties.setProperty(indexPath, "" + offset);
			try {
				properties.storeToXML(new FileOutputStream(propertiesFile), "comment");
			} catch (Exception e) {
				LOG.debug("Storing properties failed: " + e.toString());
				e.printStackTrace();
			}
			LOG.debug("properties saved");
		};

	
}
