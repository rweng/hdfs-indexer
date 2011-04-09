/**
 * Copyright (C) 2011 Robin Wenglewski <robin@wenglewski.de>
 *
 * This work is licensed under a Creative Commons Attribution-NonCommercial 3.0 Unported License:
 * http://creativecommons.org/licenses/by-nc/3.0/
 * For alternative conditions contact the author. 
 */
package com.freshbourne.hdfs.index;

import java.util.Iterator;

import org.apache.commons.logging.Log;

import com.freshbourne.multimap.btree.BTree;
import com.freshbourne.multimap.btree.BTreeModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

public class MultiMapIndex implements Index<Long> {

	private BTree<String, String> index;
	private Log logger;
	
	
	MultiMapIndex(String path){
		Injector i = Guice.createInjector(new BTreeModule(path));
		index = i.getInstance(Key.get(new TypeLiteral<BTree<String,String>>(){}));
	}
	

	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.Index#getIterator()
	 */
	@Override
	public Iterator<?> getIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.Index#save(java.lang.String)
	 */
	@Override
	public void save(String path) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.Index#add(java.lang.String[], long)
	 */
	@Override
	public void add(String[] splits, Long offset) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * @param logger the logger to set
	 */
	@Inject
	public void setLogger(Log logger) {
		this.logger = logger;
	}

	/**
	 * @return the logger
	 */
	public Log getLogger() {
		return logger;
	}

}
