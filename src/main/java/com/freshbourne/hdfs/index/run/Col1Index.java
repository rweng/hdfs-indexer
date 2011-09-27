package com.freshbourne.hdfs.index.run;

import java.io.File;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.freshbourne.hdfs.index.CSVIndex;
import com.freshbourne.multimap.btree.BTree;
import com.freshbourne.multimap.btree.BTreeModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.log4j.Logger;

public class Col1Index extends CSVIndex implements Serializable {

	private static final long serialVersionUID = 1L;

    
    /* (non-Javadoc)
      * @see com.freshbourne.hdfs.index.CSVIndex#getColumn()
      */
	@Override
	public int getColumn() {
		return 2;
	}
}
