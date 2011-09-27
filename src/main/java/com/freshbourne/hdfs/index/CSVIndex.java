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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.freshbourne.multimap.btree.BTree;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

public abstract class CSVIndex extends BTreeIndex {
	
	@SuppressWarnings({"FieldCanBeLocal"})
    private static String delimiter = "(\t| +)";

    public void initialize(InputSplit genericSplit, TaskAttemptContext context, String indexId) {
        super.initialize(genericSplit, context, "" + getColumn());
    }


    /**
	 * @return position (-1) in the array to select
	 */
	public abstract int getColumn();
    
}
