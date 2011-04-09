package com.freshbourne.hdfs.index.run;

import java.io.Serializable;

import com.freshbourne.hdfs.index.CSVIndex;
import com.freshbourne.multimap.btree.BTree;
import com.freshbourne.multimap.btree.BTreeModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

public class Col1Index extends CSVIndex<String> implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public Col1Index(String path){
		super(path);
	}

	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.CSVIndex#getColumn()
	 */
	@Override
	public int getColumn() {
		// TODO Auto-generated method stub
		return 1;
	}

	/* (non-Javadoc)
	 * @see com.freshbourne.hdfs.index.CSVIndex#createIndex()
	 */
	@Override
	public BTree<String, String> createIndex(String path) {
		Injector i = Guice.createInjector(new BTreeModule(path));
		return i.getInstance(Key.get(new TypeLiteral<BTree<String, String>>(){}));
	}
	
	
}
