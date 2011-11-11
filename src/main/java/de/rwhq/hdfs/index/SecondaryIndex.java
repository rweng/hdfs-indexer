package de.rwhq.hdfs.index;

import java.util.AbstractMap;
import java.util.Iterator;

public class SecondaryIndex<K> extends AbstractMultiFileIndex<K,Long>{
	private Iterator<Long> iterator;

	public SecondaryIndex(BTreeIndexBuilder b) {
		super(b, null);
	}

	@Override
	protected AbstractMap.SimpleEntry<K, Long> extractEntry(String line, long pos) throws ExtractionException {
		return new AbstractMap.SimpleEntry<K, Long>(keyExtractor.extract(line), pos);
	}

	@Override
	public Iterator<String> getIterator() {
		iterator = getIterator(true);
		return null;
	}
}
