package com.freshbourne.hdfs.index;

import java.util.ArrayList;
import java.util.Properties;

public class ThreadShared {
	private Index<String, String> index;
	private Properties properties;

	private ArrayList<KeyValue> keyValueList = new ArrayList<KeyValue>(1000);
	private long offset = 0;

	private boolean isFinished = false;

	synchronized public void add(String key, String string, long offset) {
		if (isFinished())
			return;

		keyValueList.add(new KeyValue(key, string));
		this.offset = offset;
	}

	ThreadShared(Index<String, String> index, Properties p) {
		this.setIndex(index);
		this.properties = p;
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
