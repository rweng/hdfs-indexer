package com.freshbourne.hdfs.index.run;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.freshbourne.hdfs.index.CSVIndex;
import com.freshbourne.hdfs.index.IndexedRecordReader;
import com.freshbourne.multimap.btree.BTree;
import com.freshbourne.multimap.btree.BTreeModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

public class Col1Index extends CSVIndex<String, String> implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(Col1Index.class);

	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.CSVIndex#getColumn()
	 */
	@Override
	public int getColumn() {
		// TODO Auto-generated method stub
		return 2;
	}

	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.CSVIndex#createIndex()
	 */
	@Override
	public CSVIndex<String, String> createIndex(String path) {
		Injector i = Guice.createInjector(new BTreeModule(path));
		index = i.getInstance(Key.get(new TypeLiteral<BTree<String, String>>(){}));
		this.loadOrInitialize();
		return this;
	}

}
