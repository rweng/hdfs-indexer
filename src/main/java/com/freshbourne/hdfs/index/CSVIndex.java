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
import com.freshbourne.multimap.btree.BTreeModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

public class CSVIndex implements Index, Serializable {
	
	private static final long serialVersionUID = 1L;
	private BTree<Integer, String> index;
	protected static final Log LOG = LogFactory.getLog(CSVIndex.class);
	
	
	public CSVIndex() {
		Injector i = Guice.createInjector(new BTreeModule("/tmp/ind"));
		index = i.getInstance(Key.get(new TypeLiteral<BTree<Integer,String>>(){}));
	}
	
	
	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.Index#load(java.lang.String)
	 */
	@Override
	public Index load(String path) {
		index.load();
		LOG.info("index loaded");
		return this;
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
	public void add(String[] splits, long offset) {
		LOG.info("adding splits");
	}

}
