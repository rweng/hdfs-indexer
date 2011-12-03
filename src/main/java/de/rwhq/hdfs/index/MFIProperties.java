package de.rwhq.hdfs.index;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import de.rwhq.btree.Range;
import de.rwhq.comparator.LongComparator;

import java.io.*;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class MFIProperties implements Serializable {
	private static final long serialVersionUID = 1L;

	private String            path;
	private List<MFIProperty> properties;

	public long getMaxPos() {
		long pos = -1;

		for (MFIProperty p : properties) {
			if (p.endPos > pos)
				pos = p.endPos;
		}

		return pos;
	}

	public int removeByPath(String path) {
		checkNotNull(path);

		int removed = 0;

		Iterator<MFIProperty> iterator = properties.iterator();
		while (iterator.hasNext()) {
			MFIProperty next = iterator.next();
			if (path.equals(next.filePath)) {
				iterator.remove();
				removed++;
			}
		}

		return removed;
	}

	public boolean exists() {
		return new File(path).exists();
	}

	/**
	 * @param pos
	 * @return MFIProperty or null, if pos is not contained
	 */
	public MFIProperty propertyForPos(long pos) {
		for(MFIProperty p : properties) {
			if (p.startPos <= pos && p.endPos >= pos) {
				return p;
			}
		}

		return null;
	}

	public SortedSet<Range<Long>> toRanges() {
		return toRanges(null, null);
	}

	public SortedSet<Range<Long>> toRanges(final Long min, final Long max) {
		final TreeSet<Range<Long>> result = Sets.newTreeSet(Range.createRangeComparator(LongComparator.INSTANCE));

		Collection<Range<Long>> transformed =
				Collections2.transform(properties, new Function<MFIProperty, Range<Long>>() {
					@Override
					public Range<Long> apply(MFIProperty input) {

						if (min == null || input.startPos >= min) {

							// TODO: we cannot really detect altering filesplit sizes
							// but since input.enPos is alway a little bit over, we just have to return it here
							//if (max == null || input.endPos <= max)
							//	return input.toRange();

							// as a replacement (for now), we need to check at least that input.startPos < max;
							if(max == null || input.startPos < max)
								return input.toRange();
						}

						return null;
					}
				});

		
		result.addAll(Collections2.filter(transformed, Predicates.notNull()));
		return result;
	}

	public MFIProperty getPropertyForRange(Range<Long> range) {
		for(MFIProperty p : properties){
			if(p.toRange().equals(range))
				return p;
		}

		return null;
	}

	public boolean contains(long startPos) {
		return propertyForPos(startPos) != null;
	}


	public static class MFIProperty implements Serializable {
		private static final long serialVersionUID = 1L;

		public String filePath;
		public Long   startPos;
		public Long   endPos;

		/** for serialization only */
		public MFIProperty() {
		}

		public MFIProperty(String filePath, Long startPos, Long endPos) {
			this.filePath = filePath;
			this.startPos = startPos;
			this.endPos = endPos;
		}


		@Override
		public boolean equals(Object other) {
			if (other instanceof MFIProperty) {
				MFIProperty p2 = (MFIProperty) other;
				return Objects.equal(filePath, p2.filePath)
						&& Objects.equal(startPos, p2.startPos)
						&& Objects.equal(endPos, p2.endPos);
			} else {
				return false;
			}
		}

		@Override
		public String toString() {
			return Objects.toStringHelper(this)
					.add("filePath", filePath)
					.add("startPos", startPos)
					.add("endPos", endPos)
					.toString();
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(filePath, startPos, endPos);
		}

		public File getFile() {
			return new File(filePath);
		}

		public Range<Long> toRange() {
			return new Range<Long>(startPos, endPos);
		}
	}

	public MFIProperties(String path) {
		this.path = path;
		this.properties = Lists.newArrayList();
	}

	public List<MFIProperty> asList() {
		return properties;
	}

	public void write() throws IOException {

		// ensure all MFIProperties have all values set
		for (MFIProperty p : properties) {
			checkNotNull(p.filePath, "All attributes of MFIProperty must be set for writing %s", toString());
			checkNotNull(p.startPos, "All attributes of MFIProperty must be set for writing %s", toString());
			checkNotNull(p.endPos, "All attributes of MFIProperty must be set for writing %s", toString());

			checkState(p.startPos < p.endPos, "MFIProperty.startPos must be < MFIProperty.endPos for writing %s",
					toString());
		}

		FileOutputStream stream = new FileOutputStream(path);
		ObjectOutputStream oStream = new ObjectOutputStream(stream);

		oStream.writeObject(this);

		oStream.close();
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("path", path)
				.add("properties", properties)
				.toString();
	}

	public void read() throws IOException {
		FileInputStream file = new FileInputStream(path);
		ObjectInputStream oStream = new ObjectInputStream(file);


		MFIProperties loaded;

		try {
			loaded = (MFIProperties) oStream.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException("error when reading object", e);
		}

		properties = loaded.asList();
	}

	public static MFIProperties read(String path) throws IOException {
		MFIProperties p = new MFIProperties(path);
		p.read();
		return p;
	}
}
