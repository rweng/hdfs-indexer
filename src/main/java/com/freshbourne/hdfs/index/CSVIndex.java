/**
 * Copyright (C) 2011 Robin Wenglewski <robin@wenglewski.de>
 *
 * This work is licensed under a Creative Commons Attribution-NonCommercial 3.0 Unported License:
 * http://creativecommons.org/licenses/by-nc/3.0/
 * For alternative conditions contact the author. 
 */
package com.freshbourne.hdfs.index;

import java.io.File;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.freshbourne.btree.BTreeFactory;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

public abstract class CSVIndex<K> extends BTreeIndex<K> {
	
	protected String delimiter;
    protected int column;

	private static Logger LOG = Logger.getLogger(CSVIndex.class);

    @Inject
    protected CSVIndex(CSVIndexBuilder<K> b) {
	    super(b);
	    this.delimiter = b.delimiter;
	    this.column = b.column;
	    if(LOG.isDebugEnabled()){
	    LOG.debug("delimiter = '" + this.delimiter + "'");
	    LOG.debug("column = " + this.column);
	    }
    }


    @Override
    public K extractKeyFromLine(String line) {
        String[] splits = line.split(delimiter);
	    if(LOG.isDebugEnabled())
	    LOG.debug("trying to transform key: '" + splits[ column ] + "'");
	    return transformToKeyType(splits[column]);
    }

	protected abstract K transformToKeyType(String key);
}
