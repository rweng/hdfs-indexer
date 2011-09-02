package com.freshbourne.hdfs.index;

import java.util.Comparator;

public class KeyValueComparator implements Comparator<KeyValue> {

	private Comparator keyComp;

	KeyValueComparator(Comparator keyComparator){
		this.keyComp = keyComparator;
	}

	@Override
	public int compare(KeyValue keyValue1, KeyValue keyValue2) {
		return keyComp.compare(keyValue1.key, keyValue2.key);
	}
}
