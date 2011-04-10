package com.freshbourne.hdfs.index.run;

import java.io.Serializable;
import java.util.Iterator;

import com.freshbourne.hdfs.index.CSVIndex;
import com.freshbourne.multimap.btree.BTree;
import com.freshbourne.multimap.btree.BTreeModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

public class Col1Index extends CSVIndex<String, String> implements Serializable {

	private static final long serialVersionUID = 1L;
	
	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.CSVIndex#getColumn()
	 */
	@Override
	public int getColumn() {
		// TODO Auto-generated method stub
		return 3;
	}

	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.CSVIndex#createIndex()
	 */
	@Override
	public BTree<String, String> createIndex(String path) {
		Injector i = Guice.createInjector(new BTreeModule(path));
		return i.getInstance(Key.get(new TypeLiteral<BTree<String, String>>(){}));
	}

	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.Index#add(java.lang.String[], java.lang.Object)
	 */
	@Override
	public void add(String[] splits, String value) {
		if(splits.length <= getColumn())
			return;
		
		add(splits[getColumn()], value);
	}
	
	
}
