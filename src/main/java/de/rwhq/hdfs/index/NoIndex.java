package de.rwhq.hdfs.index;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import de.rwhq.btree.Range;
import de.rwhq.hdfs.index.extractor.ExtractionException;
import de.rwhq.hdfs.index.extractor.KeyExtractor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

public class NoIndex<K> implements Index<K, Integer> {
	private final TreeSet ranges = Sets.newTreeSet();
	private KeyExtractor<K> keyExtractor;
	private static Log LOG = LogFactory.getLog(NoIndex.class);
	private TreeSet<Range<K>> defaultSearchRanges;
	private Comparator<K> comparator;

	public NoIndex(MFIBuilder b) {
		keyExtractor = checkNotNull(b.getKeyExtractor(), "keyExtractor is null");
		comparator = checkNotNull(b.getComparator(), "comparator is null");

		if (b.getDefaultSearchRanges() != null) {
			this.defaultSearchRanges = Range.merge(b.getDefaultSearchRanges(), comparator);
		}
	}

	@Override
	public void open() throws IOException {
	}

	@Override
	public void sync() {
	}

	@Override
	public boolean isOpen() {
		return true;
	}

	@Override
	public Iterator<String> getIterator() {
		return null;
	}

	@Override
	public void close() {
	}

	@Override
	public boolean addLine(String line, long startPos, long endPos) {
		return lineMatchesSearchRange(line);
	}

	private boolean lineMatchesSearchRange(final String line) {
		final K key;
		try {
			key = keyExtractor.extract(line);
		} catch (ExtractionException e) {
			LOG.warn("could not extract key from line: " + line, e);
			return true;
		}

		return lineMatchesSearchRange(key);
	}

	private boolean lineMatchesSearchRange(final K key) {
		Collection<Range<K>> resultCollection = Collections2.filter(defaultSearchRanges, new Predicate<Range<K>>() {
			@Override
			public boolean apply(Range<K> input) {
				return input.contains(key, comparator);
			}
		});

		return !resultCollection.isEmpty();
	}

	@Override
	public long getMaxPos() {
		return 0;
	}

	@Override
	public long partialEndForPos(long pos) {
		return 0;
	}

	@Override
	public SortedSet<Range<Long>> toRanges() {
		return ranges;
	}

	@Override
	public Iterator<String> getIterator(Range<Long> range) throws IOException {
		return null;
	}
}
