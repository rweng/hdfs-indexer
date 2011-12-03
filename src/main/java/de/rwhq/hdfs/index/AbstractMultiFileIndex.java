package de.rwhq.hdfs.index;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import de.rwhq.btree.BTree;
import de.rwhq.btree.Range;
import de.rwhq.io.rm.ResourceManager;
import de.rwhq.io.rm.ResourceManagerBuilder;
import de.rwhq.serializer.FixLengthSerializer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
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
 * The file name and path are stored in the structure in the index directory. The identifier and starting position
 * could
 * be stored directly in the file name. However, as it is unsure where the indexing will end at the time the index file
 * is created, it is not possible to store the end position in the file name (assuming we dont want to rename). Thus, a
 * properties file is required.
 */
public abstract class AbstractMultiFileIndex<K, V> implements Index<K, V> {
	private static Log LOG = LogFactory.getLog(AbstractMultiFileIndex.class);

	protected String hdfsFile;
	protected File   indexRootFolder;
	protected boolean isOpen  = false;
	protected boolean ourLock = false;
	protected AbstractMap.SimpleEntry<K, V>[] cache = null;
	protected int                             cachePointer;
	protected Comparator<K>                   comparator;
	protected FixLengthSerializer<K, byte[]>  keySerializer;
	protected KeyExtractor<K>                 keyExtractor;

	protected TreeSet<Range<K>>              defaultSearchRanges;
	private   FixLengthSerializer<V, byte[]> valueSerializer;
	private   MFIProperties                  properties;
	private   MFIProperties.MFIProperty      writingTreePropertyEntry;
	private   FileSplit                      fileSplit;
	private int cacheSize;

	/** {@inheritDoc} */
	@Override
	public boolean addLine(String line, long startPos, long endPos) {
		if (LOG.isDebugEnabled()) {
			// LOG.debug("addLine: " + line + " (pos: " + pos + ")");
		}

		ensureOpen();

		if (line.equals("")) {
			handleEmptyLine(line, startPos, endPos);
			return false;
		}

		if (!ourLock && isLocked()) {
			return lineMatchesSearchRange(line);
		} else {
			lock();

			// lazy initializing the cache
			if(cache == null)
				this.cache = ObjectArrays.newArray(AbstractMap.SimpleEntry.class, cacheSize);
		}

		// ensure not already covered by index
		//TODO: remove in production
		checkArgument(!properties.contains(startPos), "startPos already covered by index: \n" + startPos);


		if (writingTreePropertyEntry.endPos != null && writingTreePropertyEntry.endPos >= startPos) {
			throw new IllegalArgumentException(
					"expected the current position to be the largest. last pos: " +
							writingTreePropertyEntry.endPos + "; current: " + startPos);
		}

		if (cachePointer >= cache.length) {
			saveWriteTree();
		}

		if (writingTreePropertyEntry.startPos == null)
			writingTreePropertyEntry.startPos = startPos;

		writingTreePropertyEntry.endPos = endPos;

		// only add it if extraction works
		try {
			cache[cachePointer++] = extractEntry(line, startPos);
			return lineMatchesSearchRange(cache[cachePointer - 1].getKey());
		} catch (ExtractionException e) {
			LOG.error("exception when extracting '" + line + "' at position " + startPos, e);
			return true;
		}
	}

	/**
	 * an empty line could have multiple resons: it could be just an empty line, or it could be that the hdfs
	 * file got appended and now has a new line (where it wasn't terminated with one before)
	 * <p/>
	 * Independent, we ommit the line. There are three cases we got to consider:
	 * <ol>
	 * <li>we are currently creating an index ourselves and got the lock. Then we just extend this indexes coverage</li>
	 * <li>we are not an index but we can add it to a previous or next indexes coverage</li>
	 * <li>neither of both, in which case we just ommit the line. It will extend an coverage later anyway.</li>
	 * </ol>
	 */
	private void handleEmptyLine(String line, long startPos, long endPos) {

		// first check case 1.
		if (ourLock) {
			if (writingTreePropertyEntry.startPos == null)
				writingTreePropertyEntry.startPos = startPos;

			writingTreePropertyEntry.endPos = endPos;
		}

		// case 2, previous index
		MFIProperties.MFIProperty p = properties.propertyForPos(startPos - 1);
		if (p != null) {
			p.endPos = endPos;
			try {
				properties.write();
				return;
			} catch (IOException e) {
				LOG.error("could not extend index: ", e);
			}
		}

		// case 2, next index
		p = properties.propertyForPos(endPos + 1);
		if (p != null) {
			p.startPos = startPos;
			try {
				properties.write();
				return;
			} catch (IOException e) {
				LOG.error("could not extend index: ", e);
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean isOpen() {
		return isOpen;
	}


	/** {@inheritDoc} */
	@Override
	public long partialEndForPos(long pos) {
		ensureOpen();

		MFIProperties.MFIProperty property = properties.propertyForPos(pos);
		return property == null ? -1L : property.endPos;
	}

	/** {@inheritDoc} */
	@Override
	public void open() throws IOException {
		File indexDir = getIndexFolder();
		indexDir.mkdirs();

		writingTreePropertyEntry = new MFIProperties.MFIProperty();
		cachePointer = 0;

		if (properties.exists())
			properties.read();

		if (LOG.isDebugEnabled())
			LOG.debug("Index opened. Properties: " + properties);

		isOpen = true;
	}

	@Override
	public void sync() {
		ensureOpen();

		saveWriteTree();
	}

	/** {@inheritDoc} */
	@Override
	public void close() {
		LOG.info("closing index");
		LOG.info(this);
		if (!isOpen())
			return;

		sync();

		if (ourLock)
			unlock();

		isOpen = false;
	}

	/** {@inheritDoc} */
	@Override
	public long getMaxPos() {
		return properties.getMaxPos();
	}

	/** {@inheritDoc} */
	@Override
	public SortedSet<Range<Long>> toRanges() {
		SortedSet<Range<Long>> ranges =
				properties.toRanges(fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength() - 1);

		if(LOG.isDebugEnabled()){
			LOG.debug("Ranges for fileSplit:" + fileSplit.getStart() + " - " + (fileSplit.getStart() + fileSplit.getLength() - 1));
			LOG.debug(ranges);
			LOG.debug("-----");
			LOG.debug("all ranges: ");
			LOG.debug(properties.toRanges());
		}
		
		return ranges;
	}


	@Override
	public Iterator<String> getIterator() {
		ensureOpen();

		Iterator<Iterator<String>> iterator =
				Iterators.transform(toRanges().iterator(), new Function<Range<Long>, Iterator<String>>() {

					@Override
					public Iterator<String> apply(@Nullable Range<Long> input) {
						try {
							return getIterator(input);
						} catch (IOException e) {
							throw new RuntimeException("error when transforming ranges to iterators over the ranges",
									e);
						}
					}
				});

		return Iterators.concat(iterator);
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
		checkNotNull(b.getIndexRootFolder(), "index root folder must not be null");
		checkNotNull(b.getKeyExtractor(), "keyExtractor must not be null");
		checkNotNull(b.getFileSplit(), "FileSplit must not be null");

		checkState(b.getHdfsPath().startsWith("/"), "hdfsPath must start with /. Is: %s", b.getHdfsPath());
		checkState(b.getCacheSize() >= 10, "cacheSize must be >= 10");
		checkState(b.getIndexRootFolder().exists(), "index folder must exist");

		this.hdfsFile = b.getHdfsPath();
		this.keyExtractor = b.getKeyExtractor();
		this.keySerializer = b.getKeySerializer();
		this.comparator = b.getComparator();
		this.valueSerializer = valueSerializer;
		this.indexRootFolder = b.getIndexRootFolder();
		this.properties = new MFIProperties(getIndexFolder() + "/properties");
		this.keyExtractor = b.getKeyExtractor();
		this.fileSplit = b.getFileSplit();
		this.cacheSize = b.getCacheSize();

		if (b.getDefaultSearchRanges() != null) {
			this.defaultSearchRanges = Range.merge(b.getDefaultSearchRanges(), comparator);
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
	File getLockFile() {
		return new File(getIndexFolder() + "/lock");
	}

	/** @return directory of the index-files for the current hdfs file */
	@VisibleForTesting
	File getIndexFolder() {
		return new File(indexRootFolder.getPath() + hdfsFile);
	}

	protected Iterator<V> getTreeIterator(Range<Long> partial) throws IOException {
		return getTree(properties.getPropertyForRange(partial).filePath, false).getIterator(defaultSearchRanges);
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

		LOG.info("saving index: from " + writingTreePropertyEntry.startPos + " to " + writingTreePropertyEntry.endPos);

		BTree<K, V> tree;

		try {
			tree = createWritingTree();
			tree.bulkInitialize(cache, 0, cachePointer - 1, false);

			writingTreePropertyEntry.filePath = tree.getPath();

			if (LOG.isDebugEnabled())
				LOG.debug("new trees path: " + tree.getPath());

			if (properties.exists())
				properties.read();
			properties.asList().add(writingTreePropertyEntry);
			properties.write();

			writingTreePropertyEntry = new MFIProperties.MFIProperty();

			tree.close();
		} catch (IOException e) {
			LOG.error("error when saving index");
			LOG.error(e.getStackTrace());
			// reset cache and properties next, maybe we can save this index partial next time
		}

		unlock();
		cachePointer = 0;
		writingTreePropertyEntry.startPos = writingTreePropertyEntry.endPos = null;
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
		try {
			properties.read();
		} catch (IOException e) {
			LOG.error("Could not load properties, operating on old instance.", e);
		}

		// filter trees not in split range
		Collection<MFIProperties.MFIProperty> filted =
				Collections2.filter(properties.asList(), new Predicate<MFIProperties.MFIProperty>() {
					@Override
					public boolean apply(@Nullable MFIProperties.MFIProperty input) {
						if (input.endPos > fileSplit.getStart() + fileSplit.getLength() - 1) {
							return false;
						} else if (input.startPos >= fileSplit.getStart()) {
							if (input.endPos > fileSplit.getStart() + fileSplit.getLength() - 1) {
								throw new RuntimeException(
										"property must not start within filesplit range and go over it. " +
												"property: " + input + "\n" +
												"fileSplit: start = " + fileSplit.getStart() + ", end = " + (fileSplit.getStart() + fileSplit.getLength() - 1));
							} else {
								return true;
							}
						} else {
							if (input.endPos >= fileSplit.getStart()) {
								throw new RuntimeException(
										"property must not start before fileSplit.getStart() and go over it. \n" +
												"property: " + input + "\n" +
												"fileSplit: start = " + fileSplit.getStart() + ", end = " + (fileSplit.getStart() + fileSplit.getLength() - 1));
							} else {
								return false;
							}
						}
					}
				});

		// put the transformed list in a new ArrayList because Lists.transfrom returns a
		// RandomAccessList which returns different instances when calling list.get(). This results in
		// list.get(0) != list.get(0)

		Collection<BTree<K, V>> trees =
				Collections2.transform(filted, new Function<MFIProperties.MFIProperty, BTree<K, V>>() {
					@Override
					public BTree<K, V> apply(MFIProperties.MFIProperty input) {
						try {
							return getTree(input.filePath, false);
						} catch (IOException e) {
							LOG.error("error creating btree " + input.filePath, e);
						}

						return null;
					}
				});


		return Lists.newArrayList(trees);
	}

	private BTree<K, V> getTree(String filePath, boolean lock) throws IOException {

		ResourceManager rm =
				new ResourceManagerBuilder().file(filePath).open().useLock(lock).build();

		BTree<K, V> tree = BTree.create(rm, keySerializer, valueSerializer, comparator);
		tree.load();

		return tree;
	}

	protected void finalize() throws Throwable {
		try {
			close();
		} finally {
			super.finalize();
		}
	}
}
