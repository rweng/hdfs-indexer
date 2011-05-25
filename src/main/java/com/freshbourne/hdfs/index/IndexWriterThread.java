/**
 * Copyright (C) 2011 Robin Wenglewski <robin@wenglewski.de>
 *
 * This work is licensed under a Creative Commons Attribution-NonCommercial 3.0 Unported License:
 * http://creativecommons.org/licenses/by-nc/3.0/
 * For alternative conditions contact the author. 
 */
package com.freshbourne.hdfs.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.freshbourne.hdfs.index.IndexedRecordReader.Shared;

class IndexWriterThread extends Thread {
	
	private static final Log LOG = LogFactory.getLog(IndexWriterThread.class);
	private Shared shared;

	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		
		LOG.debug("Running thread");
		
		try {
			synchronized (shared) {
				shared.wait(1000 * 60 * 5); // wait 5 min	
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
		
		LOG.debug("after sleep");
		
		int c = 0;
		while(!shared.getSplitsList().isEmpty()){
			LOG.debug("Writing from thread to index: " + ++c);
			shared.getIndex().add(shared.getSplitsList().poll(), shared.getValueList().poll());
		}

		shared.save();
		LOG.debug("Ending Thread");
	}
	
	public IndexWriterThread(Shared s) {
		this.shared = s;
	}
	
}
