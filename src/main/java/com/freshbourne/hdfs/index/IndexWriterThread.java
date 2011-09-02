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

import java.util.Comparator;


/**
 * receives a sharedContainer object from the IndexRecordReader to handle everything, the IndexRecordReader doesn't want to do:
 * sorting the Array and writing the tree to disk.
 * 
 */
class IndexWriterThread extends Thread {
	
	private static final Log LOG = LogFactory.getLog(IndexWriterThread.class);
	private SharedContainer sharedContainer;

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		LOG.debug("Running thread");

		sharedContainer.getKeyValueList();
		sort();
		sharedContainer.getIndex().bulkInsert(sharedContainer);
		
		LOG.debug("Ending Thread");
	}

	private void sort() {
		Comparator<String> comparator = sharedContainer.getIndex().getKeyComparator();
		java.util.Collections.sort(sharedContainer.getKeyValueList(), new KeyValueComparator(comparator));
	}

	public IndexWriterThread(SharedContainer s) {
		this.sharedContainer = s;
	}

	synchronized public void save(){
		/*
			LOG.debug("saving index");
			index.close();
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
			*/
		}

}
