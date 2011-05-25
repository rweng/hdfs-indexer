/**
 * Copyright (C) 2011 Robin Wenglewski <robin@wenglewski.de>
 *
 * This work is licensed under a Creative Commons Attribution-NonCommercial 3.0 Unported License:
 * http://creativecommons.org/licenses/by-nc/3.0/
 * For alternative conditions contact the author. 
 */
package com.freshbourne.hdfs.index;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.freshbourne.hdfs.index.IndexedRecordReader.Shared;
import com.freshbourne.hdfs.index.IndexedRecordReader.SplitsValue;

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
			SplitsValue sv = shared.getSplitsValueList().poll(1, TimeUnit.MINUTES);
			while (!shared.isFinished() && sv != null) {
				shared.getIndex().add(sv.getSplits(), sv.getValue());
				sv = shared.getSplitsValueList().poll(1, TimeUnit.MINUTES);	
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
	
}
