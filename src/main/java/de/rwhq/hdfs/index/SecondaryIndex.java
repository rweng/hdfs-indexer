package de.rwhq.hdfs.index;

import de.rwhq.serializer.LongSerializer;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public class SecondaryIndex<K> extends AbstractMultiFileIndex<K, Long> {
	private LineRecordReader recordReader;

	public SecondaryIndex(BTreeIndexBuilder b) {
		super(b, LongSerializer.INSTANCE);
		this.recordReader = b.getRecordReader();
	}

	@Override
	protected AbstractMap.SimpleEntry<K, Long> extractEntry(String line, long pos) throws ExtractionException {
		return new AbstractMap.SimpleEntry<K, Long>(keyExtractor.extract(line), pos);
	}

	@Override
	public Iterator<String> getIterator() {
		checkNotNull(recordReader, "recordReader must not be null when iterating over a secondary index");
		return new SecondaryIndexIterator(getIterator(true));
	}

	class SecondaryIndexIterator implements Iterator<String> {

		private Iterator<Long> iterator;

		public SecondaryIndexIterator(Iterator<Long> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public String next() {
			Long nextLong = iterator.next();

			if(nextLong == null)
				return null;

			long oldPos = recordReader.pos;
			recordReader.pos = nextLong;
			try {
				recordReader.nextKeyValue();
			} catch (IOException e) {
				throw new RuntimeException("error when reading entry from hdfs", e);
			}

			String result = recordReader.getCurrentValue().toString();
			recordReader.pos = oldPos;

			return result;
		}

		@Override
		public void remove() {
			iterator.remove();
		}
	}
}
