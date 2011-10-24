package com.freshbourne.hdfs.index;

import com.freshbourne.btree.BTree;
import com.freshbourne.btree.BTreeFactory;
import com.freshbourne.btree.Range;
import com.freshbourne.serializer.FixLengthSerializer;
import com.freshbourne.serializer.FixedStringSerializer;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

/**
 * This is the base class for all Indexes using the multimap bTreeWriting.
 * <p/>
 * Instances should be created through Guice. Reason for this are manifold.
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
public abstract class BTreeIndex<K> implements Index<K, String>, Serializable {

	private static final long serialVersionUID = 1L;
	private static       Log  LOG              = LogFactory.getLog(BTreeIndex.class);

	private String     indexId;
	private String     hdfsFile;
	private File       indexRootFolder;
	private Properties properties;

	private boolean isOpen = false;
	private BTreeFactory factory;
	private boolean ourLock = false;
	private Comparator<K>                        comparator;
	private FixLengthSerializer<K, byte[]>       keySerializer;
	private List<Range<K>>                       defaultSearchRanges;
	private AbstractMap.SimpleEntry<K, String>[] cache;
	private PropertyEntry writingTreePropertyEntry = new PropertyEntry();
	private int cacheSize;
	private int cachePointer = 0;

	@Inject
	protected BTreeIndex(BTreeIndexBuilder<K> b) {
		// if hdfsFile doesn't start with /, the server name is before the path
		this.hdfsFile = b.hdfsFile.replaceAll("^hdfs://[^/]*", "");

		this.indexRootFolder = b.indexFolder;
		this.indexId = b.indexId;
		this.factory = b.factory;
		this.keySerializer = b.keySerializer;
		this.comparator = b.comparater;
		this.defaultSearchRanges = b.defaultSearchRanges;
		this.cacheSize = b.cacheSize;
	}

	public boolean isOpen() {
		return isOpen;
	}

	public void open() throws IOException {
		File indexDir = getIndexDir();
		indexDir.mkdirs();

		loadOrCreateProperties();

		this.cache = new AbstractMap.SimpleEntry[cacheSize];
		isOpen = true;
	}


	@Override
	public void close() {
		if (!isOpen())
			return;

		saveWriteTree();

		if (ourLock)
			unlock();
		isOpen = false;
	}


	protected void ensureOpen() {
		if (!isOpen())
			throw new IllegalStateException("index must be opened before it is used");
	}

	private Properties getProperties() {
		if (properties != null) {
			return properties;
		}

		properties = new Properties();
		File propertiesFile = new File(indexRootFolder + "properties");
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

	String getPropertiesPath() {
		return getIndexDir() + "/properties.xml";
	}

	private void saveProperties() throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("saving properties:\n " + properties);
		properties.storeToXML(new FileOutputStream(getPropertiesPath()), "comment");
	}

	private Properties loadOrCreateProperties() throws IOException {
		properties = new Properties();

		try {
			properties.loadFromXML(new FileInputStream(getPropertiesPath()));
		} catch (IOException e) {
			saveProperties();
		}

		return properties;
	}

	/** @return directory of the index-files for the current hdfs file */
	File getIndexDir() {
		return new File(indexRootFolder.getPath() + hdfsFile);
	}

	String getHdfsFile() {
		return hdfsFile;
	}


	private List<BTree<K, String>> getTreeList() {
		List<BTree<K, String>> list = new LinkedList<BTree<K, String>>();

		// add trees from properties
		for (String filename : getProperties().stringPropertyNames()) {
			list.add(getTree(new File(getIndexDir() + "/" + filename)));
		}

		return list;
	}

	@Override
	public Iterator<String> getIterator() {
		return getIterator(true);
	}

	public Iterator<String> getIterator(List<Range<K>> searchRange) {
		ensureOpen();
		return new BTreeIndexIterator(searchRange);
	}

	public Iterator<String> getIterator(boolean useDefaultSearchRanges) {
		ensureOpen();
		if (useDefaultSearchRanges)
			return new BTreeIndexIterator(defaultSearchRanges);
		else
			return new BTreeIndexIterator();
	}

	private void unlock() {
		if (ourLock) {
			getLockFile().delete();
			ourLock = false;
		}
	}

	@Override
	public void addLine(String line, long pos) {
		ensureOpen();
		if (!ourLock && isLocked()) {
			return;
		} else {
			lock();
		}

		if (cachePointer >= cacheSize) {
			saveWriteTree();
		}

		K key = extractKeyFromLine(line);
		if (writingTreePropertyEntry.start == null)
			writingTreePropertyEntry.start = pos;

		writingTreePropertyEntry.end = pos;
		cache[cachePointer++] = new AbstractMap.SimpleEntry<K, String>(key, line);
	}

	private void saveWriteTree() {

		if (cachePointer == 0)
			return;

		if (LOG.isDebugEnabled())
			LOG.debug("bulkInitializing tree");

		BTree<K, String> tree = null;

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

	private boolean isEnoughMemory() {
		long free = Runtime.getRuntime().freeMemory() / 1024 / 1024;
		if (free < 3) {
			LOG.info("collecting garbarge");
			collectGarbage();
			collectGarbage();
		}

		if (free < 5) {
			return false;
		} else {
			return true;
		}
	}

	private static int fSLEEP_INTERVAL = 100;

	private static void collectGarbage() {
		try {
			System.gc();
			Thread.currentThread().sleep(fSLEEP_INTERVAL);
			System.runFinalization();
			Thread.currentThread().sleep(fSLEEP_INTERVAL);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}
	}

	private boolean isLocked() {
		return getLockFile().exists();
	}

	private void lock() {
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

	protected void finalize() throws Throwable {
		try {
			unlock();
			close();
		} finally {
			super.finalize();
		}
	}

	File getLockFile() {
		return new File(getIndexDir() + "/lock");
	}

	private BTree<K, String> createWritingTree() throws IOException {
		String file = getIndexDir() + "/" + indexId + "_" + System.currentTimeMillis();
		if (LOG.isDebugEnabled())
			LOG.debug("trying to create btree: " + file);

		BTree<K, String> tree = factory.get(new File(file), keySerializer, FixedStringSerializer.INSTANCE_1000,
				comparator, false);

		if (LOG.isDebugEnabled())
			LOG.debug("creeated btree for writing: " + tree.getPath());

		return tree;
	}

	private BTree<K, String> getTree(File file) {
		BTree<K, String> result = null;
		try {
			result = factory.get(file, keySerializer, FixedStringSerializer.INSTANCE_1000,
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


	@Override
	public boolean exists() {
		throw new UnsupportedOperationException("todo: check if directory and properties file exists");
	}


	@Override public long getMaxPos() {
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
	 * This method implemented by a subclass returns the key for a given line.
	 * <p/>
	 * This method isn't perfect since it assumes that each line is one entry. Maybe this can be made more generic later!
	 * <p/>
	 * Also, call ensureOpen() in this method
	 *
	 * @param line
	 * 		in the hdfs file
	 * @return key or null to ignore the line
	 */
	public abstract K extractKeyFromLine(String line);

	public class PropertyEntry {
		private Long start = null;
		private Long end   = null;

		public PropertyEntry() {

		}

		public PropertyEntry(long start, long end) {
			this.end = end;
			this.start = start;
		}

		public String toString() {
			String str = "";
			if (start != null)
				str += start;
			str += ";";
			if (end != null)
				str += end;
			return str;
		}

		public void loadFromString(String s) {
			String[] splits = s.split(";");
			start = Long.parseLong(splits[0]);
			end = Long.parseLong(splits[1]);
		}
	}

	class BTreeIndexIterator implements Iterator<String> {

		private List<BTree<K, String>> trees;
		private BTree<K, String>       currentTree;
		private Iterator<String>       currentIterator;
		private List<Range<K>>         searchRanges;


		private BTreeIndexIterator() {
			this.trees = getTreeList();
		}

		public BTreeIndexIterator(List<Range<K>> searchRange) {
			this.searchRanges = searchRange;
			this.trees = getTreeList();
		}

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
					currentIterator = currentTree.getIterator(searchRanges);
				}

				return hasNext();
			} catch (Exception e) {
				// if anything went wrong, log and remove tree
				LOG.error("Error in BTreeIndexIterator#hasNext()", e);

				try {
					currentTree.close();
				} catch (IOException ignore) {
					LOG.error("error closing currentTree", e);
				}

				String path = currentTree.getPath();
				String[] splits = path.split("\\/");
				new File(path).delete();
				getProperties().remove(splits[splits.length - 1]);
				try {
					saveProperties();
				} catch (IOException e1) {
					LOG.error("error saving properties after deleting not-working tree", e);
				}

				return hasNext();
			}
		}

		@Override
		public String next() {
			if (hasNext()) {
				return currentIterator.next();
			}

			return null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
