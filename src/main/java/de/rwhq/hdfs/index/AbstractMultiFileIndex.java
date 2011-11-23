package de.rwhq.hdfs.index;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ObjectArrays;
import de.rwhq.btree.BTree;
import de.rwhq.btree.Range;
import de.rwhq.io.rm.ResourceManager;
import de.rwhq.io.rm.ResourceManagerBuilder;
import de.rwhq.serializer.FixLengthSerializer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This is the base class for all Indexes using the multimap bTreeWriting.
 * <p/>
 * The folder structure and files are not created when creating an instance of this class. Rather, you need to open the
 * class explicitly through the open() method.
 * <p/>
 * Also, to ensure that data is really written to the disk, close has to be called as specified by the Index interface.
 * <p/>
 * Some methods, like exists() work without an opened instance. Most methods, however, require that the instance was
 * opened first.
 * <p/>
 * The instance gets a path in which all btrees are stored. The folder structure within this index-path is acording to
 * the files of the HDFS.
 * <p/>
 * Example: If the index-path is /data/indexes, and an index is created over the second column of
 * hdfs:///csvs/users.csv, the index files that are created are:
 * <p/>
 * /data/indexes/csvs/users.csv/properties.xml /data/indexes/csvs/users.csv/2_0 /data/indexes/csvs/users.csv/2_10005
 * <p/>
 * With each bTreeWriting, there are four information that have to be stored:
 * <p/>
 * - the hdfs file name and path - the identifier to be indexed (column, xml-path, ...) - the starting position in the
 * hdfs file for the index - the end position in the hdfs file for the index
 * <p/>
 * The file name and path are stored in the structure in the index directory. The identifier and starting position could
 * be stored directly in the file name. However, as it is unsure where the indexing will end at the time the index file
 * is created, it is not possible to store the end position in the file name (assuming we dont want to rename). Thus, a
 * properties file is required.
 */
public abstract class AbstractMultiFileIndex<K, V> implements Index<K, V> {
	private static Log LOG = LogFactory.getLog(AbstractMultiFileIndex.class);

	protected Properties properties;

	protected File   propertiesFile;
	protected String hdfsFile;
	protected File   indexRootFolder;
	protected boolean                    isOpen                   = false;
	protected PrimaryIndex.PropertyEntry writingTreePropertyEntry = new PrimaryIndex.PropertyEntry();
	protected boolean                    ourLock                  = false;
	protected AbstractMap.SimpleEntry<K, V>[] cache;
	protected int                             cacheSize;
	protected int cachePointer = 0;
	protected Comparator<K>                  comparator;
	protected FixLengthSerializer<K, byte[]> keySerializer;
	protected KeyExtractor<K>                keyExtractor;

	/* if an exception occured during key extraction */
	private boolean extractorException = false;
	protected List<Range<K>>                 defaultSearchRanges;
	private   FixLengthSerializer<V, byte[]> valueSerializer;

	/** {@inheritDoc} */
	@Override
	public boolean addLine(String line, long pos) {
		if (LOG.isDebugEnabled()) {
			// LOG.debug("addLine: " + line + " (pos: " + pos + ")");
		}

		ensureOpen();


		if (!ourLock && isLocked()) {
			LOG.info("index is locked.");
			return lineMatchesSearchRange(line);
		} else {
			lock();
		}

		if (writingTreePropertyEntry.end != null && writingTreePropertyEntry.end >= pos) {
			throw new IllegalArgumentException(
					"expected the current position to be the largest. last pos: " +
							writingTreePropertyEntry.end + "; current: " + pos);
		}

		if (cachePointer >= cacheSize) {
			saveWriteTree();
		}

		// LOG.info("adding tree to cache");
		if (writingTreePropertyEntry.start == null)
			writingTreePropertyEntry.start = pos;

		writingTreePropertyEntry.end = pos;

		// only add it if extraction works
		try {
			cache[cachePointer++] = extractEntry(line, pos);
			return lineMatchesSearchRange(cache[cachePointer - 1].getKey());
		} catch (ExtractionException e) {
			LOG.error("exception when extracting '" + line + "' at position " + pos, e);
			return true;
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean isOpen() {
		return isOpen;
	}

	/** {@inheritDoc} */
	@Override
	public void open() throws IOException {
		File indexDir = getIndexFolder();
		indexDir.mkdirs();

		loadOrCreateProperties();

		this.cache = ObjectArrays.newArray(AbstractMap.SimpleEntry.class, cacheSize);
		isOpen = true;
	}

	/** {@inheritDoc} */
	@Override
	public void close() {
		LOG.info("closing index");
		LOG.info(this);
		if (!isOpen())
			return;

		saveWriteTree();

		if (ourLock)
			unlock();

		isOpen = false;
	}

	/** {@inheritDoc} */
	@Override
	public long getMaxPos() {
		long largest = 0;

		for (String filename : getProperties().stringPropertyNames()) {
			String propertyStr = getProperties().getProperty(filename, null);

			PropertyEntry p = new PropertyEntry();

			if (propertyStr != null)
				p.loadFromString(propertyStr);

			if (largest < p.end)
				largest = p.end;

		}
		return largest;
	}

	/**
	 * Constructor does not check values, this should be done in the BTreeIndexBuilder
	 *
	 * @param b
	 * 		BTreeIndexBuilder
	 * @param valueSerializer
	 */
	public AbstractMultiFileIndex(BTreeIndexBuilder b, FixLengthSerializer<V, byte[]> valueSerializer) {

		checkNotNull(b.getHdfsPath(), "hdfsPath is null");
		checkNotNull(b.getKeyExtractor(), "keyExtractor is null");
		checkNotNull(b.getKeySerializer(), "keySerializer is null");
		checkNotNull(b.getComparator(), "comparator is null");
		checkNotNull(valueSerializer, "valueSerializer must not be null");
		checkNotNull(b.getIndexFolder(), "index root folder must not be null");
		checkNotNull(b.getKeyExtractor(), "keyExtractor must not be null");

		checkState(b.getHdfsPath().startsWith("/"), "hdfsPath must start with /");
		checkState(b.getCacheSize() >= 100, "cacheSize must be >= 100");
		checkState(b.getIndexFolder().exists(), "index folder must exist");

		this.hdfsFile = b.getHdfsPath();
		this.keyExtractor = b.getKeyExtractor();
		this.keySerializer = b.getKeySerializer();
		this.comparator = b.getComparator();
		this.valueSerializer = valueSerializer;
		this.indexRootFolder = b.getIndexFolder();
		this.cacheSize = b.getCacheSize();
		this.propertiesFile = new File(getIndexFolder() + "/properties.xml");
		this.keyExtractor = b.getKeyExtractor();

		if (b.getDefaultSearchRanges() != null) {
			this.defaultSearchRanges = b.getDefaultSearchRanges();
			Range.merge(defaultSearchRanges, comparator);
		}
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("isOpen", isOpen())
				.add("locked", isLocked())
				.add("ourLock", ourLock)
				.add("cacheSize", cacheSize)
				.add("defaultSearchRanges", defaultSearchRanges)
				.toString();
	}


	@VisibleForTesting
	File getPropertiesFile() {
		return propertiesFile;
	}

	@VisibleForTesting
	File getLockFile() {
		return new File(getIndexFolder() + "/lock");
	}

	/** @return directory of the index-files for the current hdfs file */
	@VisibleForTesting
	File getIndexFolder() {
		return new File(indexRootFolder.getPath() + hdfsFile);
	}

	@VisibleForTesting
	Iterator<V> getIterator(List<Range<K>> searchRange) {
		ensureOpen();
		return new BTreeIndexIterator(searchRange);
	}

	@VisibleForTesting
	Iterator<V> getIterator(boolean useDefaultSearchRanges) {
		ensureOpen();
		if (useDefaultSearchRanges)
			return new BTreeIndexIterator(defaultSearchRanges);
		else
			return new BTreeIndexIterator();
	}

	class PropertyEntry {
		protected Long start = null;
		protected Long end   = null;

		public String toString() {
			String str = "";
			if (start != null)
				str += start;
			str += ";";
			if (end != null)
				str += end;
			return str;
		}

		protected PropertyEntry() {

		}

		protected PropertyEntry(long start, long end) {
			this.end = end;
			this.start = start;
		}

		protected void loadFromString(String s) {
			String[] splits = s.split(";");
			start = Long.parseLong(splits[0]);
			end = Long.parseLong(splits[1]);
		}
	}

	class BTreeIndexIterator implements Iterator<V> {

		private List<BTree<K, V>> trees;
		private BTree<K, V>       currentTree;
		private Iterator<V>       currentIterator;
		private List<Range<K>>    searchRanges;
		private int exceptionCount = 0;

		@Override
		public boolean hasNext() {
			try {
				if (trees.size() == 0) {
					return false;
				}

				// initial tree
				if (currentTree == null)
					currentTree = trees.get(0);

				if (currentIterator == null) {
					if(searchRanges == null)
						currentIterator = currentTree.getIterator();
					else
						currentIterator = currentTree.getIterator(searchRanges);
				}

				if (currentIterator.hasNext()) {
					return true;
				}

				// try to get next tree
				int nextTree = trees.indexOf(currentTree) + 1;
				if (nextTree >= trees.size()) {
					return false;
				} else {
					currentTree = trees.get(nextTree);
					if(searchRanges != null)
						currentIterator = currentTree.getIterator(searchRanges);
					else
						currentIterator = currentTree.getIterator();
				}

				return hasNext();
			} catch (Exception e) {
				// if anything went wrong, log and remove tree
				LOG.error("Error in BTreeIndexIterator#hasNext()", e);
				exceptionCount++;

				try {
					currentTree.close();
				} catch (IOException ignore) {
					LOG.error("error closing currentTree", e);
				}

				String path = currentTree.getPath();
				String[] splits = path.split("/");
				new File(path).delete();
				getProperties().remove(splits[splits.length - 1]);
				try {
					saveProperties();
				} catch (IOException e1) {
					LOG.error("error saving properties after deleting not-working tree", e);
				}

				if (exceptionCount <= 5)
					return hasNext();
				else
					return false;
			}
		}

		@Override
		public V next() {
			if (hasNext()) {
				return currentIterator.next();
			}

			return null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		public BTreeIndexIterator(List<Range<K>> searchRange) {
			this.searchRanges = searchRange;
			this.trees = getTreeList();
		}


		private BTreeIndexIterator() {
			this.trees = getTreeList();
		}
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


	protected void ensureOpen() {
		if (!isOpen())
			throw new IllegalStateException("index must be opened before it is used");
	}

	protected abstract AbstractMap.SimpleEntry<K, V> extractEntry(String line, long pos) throws ExtractionException;

	protected void saveWriteTree() {

		if (cachePointer == 0)
			return;

		LOG.info("saving index: from " + writingTreePropertyEntry.start + " to " + writingTreePropertyEntry.end);

		BTree<K, V> tree;

		try {
			tree = createWritingTree();
			tree.bulkInitialize(cache, 0, cachePointer - 1, false);

			String filename = new File(tree.getPath()).getName();

			if (LOG.isDebugEnabled())
				LOG.debug("new trees filename: " + filename);

			getProperties().setProperty(filename, writingTreePropertyEntry.toString());
			saveProperties();

			tree.close();
		} catch (IOException e) {
			LOG.error("error when saving index");
			LOG.error(e.getStackTrace());
			// reset cache and properties next, maybe we can save this index partial next time
		}

		cachePointer = 0;
		writingTreePropertyEntry.start = writingTreePropertyEntry.end = null;
	}

	protected void saveProperties() throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("saving properties:\n " + properties);

		properties.storeToXML(new FileOutputStream(propertiesFile), "comment");
	}

	protected boolean isLocked() {
		return getLockFile().exists();
	}

	private void unlock() {
		if (ourLock) {
			getLockFile().delete();
			ourLock = false;
		}
	}

	protected void lock() {
		if (ourLock)
			return;

		if (LOG.isDebugEnabled())
			LOG.debug("locking file: " + getLockFile());
		try {
			FileUtils.touch(getLockFile());
			ourLock = true;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected Properties loadOrCreateProperties() throws IOException {
		properties = new Properties();

		try {
			properties.loadFromXML(new FileInputStream(propertiesFile));
		} catch (IOException e) {
			saveProperties();
		}

		return properties;
	}

	protected Properties getProperties() {
		if (properties != null) {
			return properties;
		}

		properties = new Properties();
		if (propertiesFile.exists()) {
			try {
				FileInputStream fis = new FileInputStream(propertiesFile);
				properties.loadFromXML(fis);
			} catch (Exception e) {
				LOG.warn("deleting properties file");
				propertiesFile.delete();
			}
		}

		return properties;
	}

	private BTree<K, V> createWritingTree() throws IOException {
		String file = getIndexFolder() + "/" + keyExtractor.getId() + "_" + System.currentTimeMillis();

		if (LOG.isDebugEnabled())
			LOG.debug("trying to build btree: " + file);

		ResourceManager manager =
				new ResourceManagerBuilder().file(file).build();
		BTree<K, V> tree = BTree.create(manager, keySerializer, valueSerializer,
				comparator);

		if (LOG.isDebugEnabled())
			LOG.debug("creeated btree for writing: " + tree.getPath());

		return tree;
	}

	private List<BTree<K, V>> getTreeList() {
		List<BTree<K, V>> list = new LinkedList<BTree<K, V>>();

		// add trees from properties
		for (String filename : getProperties().stringPropertyNames()) {
			list.add(getTree(new File(getIndexFolder() + "/" + filename)));
		}

		return list;
	}

	private BTree<K, V> getTree(File file) {
		BTree<K, V> result;
		try {
			ResourceManager manager =
					new ResourceManagerBuilder().file(file).useLock(false).build();

			result = BTree.create(manager, keySerializer, valueSerializer,
					comparator);
			result.loadOrInitialize();

			// checkStructure is very expensive, so do not do this usually
			if (LOG.isDebugEnabled())
				result.checkStructure();

		} catch (IOException e) {
			throw new RuntimeException("error occured while trying to get btree: " + file.getAbsolutePath(), e);
		}
		return result;
	}

	protected void finalize() throws Throwable {
		try {
			close();
		} finally {
			super.finalize();
		}
	}
}
