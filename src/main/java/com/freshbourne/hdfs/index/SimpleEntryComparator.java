package com.freshbourne.hdfs.index;

import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;

public class SimpleEntryComparator implements Comparator<SimpleEntry> {

	private Comparator keyComp;

	SimpleEntryComparator(Comparator keyComparator){
		this.keyComp = keyComparator;
	}

	@Override
	public int compare(SimpleEntry keyValue1, SimpleEntry keyValue2) {
		return keyComp.compare(keyValue1.getKey(), keyValue2.getKey());
	}
}
