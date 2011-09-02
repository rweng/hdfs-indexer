package com.freshbourne.hdfs.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Properties;

public class SharedContainer {
	private static final Log LOG = LogFactory.getLog(SharedContainer.class);
	private Index<String, String> index;
	private Properties properties;

	private ArrayList<KeyValue> keyValueList;
	private long offset = 0;

	private boolean isFinished = false;
	private int arraySize;

	synchronized public void add(String line, long offset) {
		if (isFinished())
			return;

		index.parseEntry(line);
		this.add(index.getCurrentParsedKey(), index.getCurrentParsedValue(), offset);
		if(keyValueList.size() >= arraySize)
			setFinished(true);
	}

	SharedContainer() {
		this(100000);
	}

	SharedContainer(int arraySize){
		keyValueList = new ArrayList<KeyValue>(arraySize);
		this.arraySize = arraySize;
	}


	public void add(String currentParsedKey, String currentParsedValue, long pos) {
		if(isFinished())
			return;

		KeyValue kv = new KeyValue(currentParsedKey, currentParsedValue);
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
	public ArrayList<KeyValue> getKeyValueList() {
		return keyValueList;
	}

}
