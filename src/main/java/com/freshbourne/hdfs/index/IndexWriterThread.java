/**
 * Copyright (C) 2011 Robin Wenglewski <robin@wenglewski.de>
 *
 * This work is licensed under a Creative Commons Attribution-NonCommercial 3.0 Unported License:
 * http://creativecommons.org/licenses/by-nc/3.0/
 * For alternative conditions contact the author. 
 */
package com.freshbourne.hdfs.index;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.freshbourne.hdfs.index.IndexedRecordReader.Shared;

class IndexWriterThread extends Thread {
	
	private ArrayList<Shared> sharedList = new ArrayList<Shared>();
	private Shared shared;
	private static String pidFile = "/tmp/indexWriter.pid";
	private static IndexWriterThread INSTANCE;
	private static final Log LOG = LogFactory.getLog(IndexWriterThread.class);

	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		try {
			Thread.sleep(1000 * 60 * 1);
		} catch (InterruptedException e) {
			new RuntimeException(e);
		}
		
		while(!shared.getSplitsList().isEmpty()){
			shared.getIndex().add(shared.getSplitsList().poll(), shared.getValueList().poll());
		}
		
		// exit
	}
	
	public IndexWriterThread(Shared s) {
		this.shared = s;
		setDaemon(true);
	}
	
}
