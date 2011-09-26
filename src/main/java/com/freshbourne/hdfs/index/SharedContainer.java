package com.freshbourne.hdfs.index;

import java.util.ArrayList;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;

public class SharedContainer<K, V> {
	// private static final Log LOG = LogFactory.getLog(SharedContainer.class);
	private Index<String, String> index;
	// private Properties properties;

	private List<SimpleEntry<K, V>> keyValueList;
	private long offset = 0;

	private boolean isFinished = false;
	private int arraySize;

	SharedContainer() {
		this(100000);
	}

	SharedContainer(int arraySize){
		keyValueList = new ArrayList<SimpleEntry<K, V>>(arraySize);
		this.arraySize = arraySize;
	}


	public void add(K currentParsedKey, V currentParsedValue, long pos) {
		if(isFinished())
			return;

		SimpleEntry<K, V> kv = new SimpleEntry<K, V>(currentParsedKey, currentParsedValue);
		keyValueList.add(kv);
		this.offset = pos;
	}

	/**
	 * @param index the index to set
	 */
	public void setIndex(Index<String, String> index) {
		this.index = index;
	}

	/**
	 * @return the index
	 */
	public Index<String, String> getIndex() {
		return index;
	}

	/**
	 * @param isFinished the isFinished to set
	 */
	public void setFinished(boolean isFinished) {
		this.isFinished = isFinished;
	}

	/**
	 * @return the isFinished
	 */
	public boolean isFinished() {
		return isFinished;
	}

	/**
	 * @return the keyValueList
	 */
	public List<SimpleEntry<K, V>> getKeyValueList() {
		return keyValueList;
	}

}
