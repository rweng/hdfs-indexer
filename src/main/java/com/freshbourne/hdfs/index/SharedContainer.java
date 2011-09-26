package com.freshbourne.hdfs.index;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;


public class SharedContainer<K, V> {
	// private static final Log LOG = LogFactory.getLog(SharedContainer.class);
	private Index<K, V> index;
	// private Properties properties;

	private List<SimpleEntry<K, V>> keyValueList;
	private long offset = 0;

	private boolean isFinished = false;
	private int arraySize;

    public SharedContainer(Index<K, V> index) {
        keyValueList = new ArrayList<SimpleEntry<K, V>>(arraySize);
        this.arraySize = 10000;

        if(index == null)
            throw new IllegalArgumentException("index is null");

        this.index = index;
    }


    public void add(K currentParsedKey, V currentParsedValue, long pos) {
		if(isFinished())
			return;

		SimpleEntry<K, V> kv = new SimpleEntry<K, V>(currentParsedKey, currentParsedValue);
		keyValueList.add(kv);
		this.offset = pos;
	}

	/**
	 * @return the index
	 */
	public Index<K, V> getIndex() {
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

    public void add(String s, long pos) {
        AbstractMap.SimpleEntry<K, V> keyValue = getIndex().parseEntry(s);
        add(keyValue.getKey(), keyValue.getValue(), pos);
    }
}
