/**
 * Copyright (C) 2011 Robin Wenglewski <robin@wenglewski.de>
 *
 * This work is licensed under a Creative Commons Attribution-NonCommercial 3.0 Unported License:
 * http://creativecommons.org/licenses/by-nc/3.0/
 * For alternative conditions contact the author. 
 */
package com.freshbourne.hdfs.index;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.freshbourne.multimap.btree.BTree;

public abstract class CSVIndex<K, V> implements Index<K, V>, Serializable {
	
	private static final long serialVersionUID = 1L;
	protected BTree<K, V> index;
	protected static final Log LOG = LogFactory.getLog(CSVIndex.class);
	
	
	public abstract CSVIndex<K, V> createIndex(String path);

	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.Index#getIterator()
	 */
	@Override
	public Iterator<V> getIterator() {
		return index.getIterator();
	}
	

	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.Index#getIterator(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Iterator<V> getIterator(K start, K end) {
		return index.getIterator(start, end);
	}

	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.Index#close(java.lang.String)
	 */
	@Override
	public void close() {
		LOG.debug("saving index");
		index.sync();
		LOG.debug("index saved");
	}
	
	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.Index#add(java.lang.String[], long)
	 */
	@Override
	public void add(K key, V value) {
		index.add(key, value);
	}
	
	/**
	 * @return position (-1) in the array to select
	 */
	public abstract int getColumn();
	
	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.Index#load(java.lang.String)
	 */
	public Index<K, V> loadOrInitialize() {
		LOG.info("creating injector and index");
		
		boolean loaded = false;
		try{
			index.load();
			loaded = true;
		} catch (Exception ignored) {
		} 
		
		if(!loaded)
			index.initialize();
		
		LOG.info("index created");
		return this;
	}
	

	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.Index#getIdentifier()
	 */
	@Override
	public String getIdentifier() {
		return "" + getColumn();
	}

}
