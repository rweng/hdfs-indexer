package de.rwhq.hdfs.index;

import de.rwhq.btree.Range;
import de.rwhq.hdfs.index.extractor.ExtractionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;

public class PrimaryIndex<K> extends AbstractMultiFileIndex<K, String> {

	private static Log LOG = LogFactory.getLog(PrimaryIndex.class);

	protected PrimaryIndex(IndexBuilder b) {
		super(b);
	}

	@Override
	protected AbstractMap.SimpleEntry<K, String> extractEntry(String line, long pos) throws ExtractionException {
		return new AbstractMap.SimpleEntry<K, String>(keyExtractor.extract(line), line);
	}

	@Override
	public Iterator<String> getIterator(Range<Long> range) throws IOException {
		return getTreeIterator(range);
	}
}
