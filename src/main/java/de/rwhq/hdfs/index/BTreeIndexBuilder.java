package de.rwhq.hdfs.index;

import com.google.common.collect.Lists;
import de.rwhq.btree.Range;
import de.rwhq.serializer.FixLengthSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class BTreeIndexBuilder<K,V> {

	private int cacheSize = 1000;
	private File indexRootFolder;
	private FixLengthSerializer<K,byte[]> keySerializer;
	private Comparator<K>  comparator;
	private List<Range<K>> defaultSearchRanges;
	private FSDataInputStream inputStream;
	private KeyExtractor keyExtractor;
	private String       hdfsPath;
	private Configuration jobConfiguration;
	private FileSplit fileSplit;

	private boolean primaryIndex = false;
	private int secondaryIndexReadBufferSize = 500;

	public FileSplit getFileSplit() {
		return fileSplit;
	}

	public Configuration getJobConfiguration() {
		return jobConfiguration;
	}

	public BTreeIndexBuilder<K, V> defaultSearchRanges(List<Range<K>> ranges){
		this.defaultSearchRanges = ranges;
		return this;
	}

	public BTreeIndexBuilder<K, V> jobConfiguration(Configuration jobConfiguration) {
		this.jobConfiguration = jobConfiguration;
		return this;
	}

	public BTreeIndexBuilder secondaryIndexReadBufferSize(int secondaryIndexReadBufferSize) {
		this.secondaryIndexReadBufferSize = secondaryIndexReadBufferSize;
		return this;
	}

	public Comparator getComparator() {
		return comparator;
	}

	public BTreeIndexBuilder comparator(Comparator comparator) {
		this.comparator = comparator;
		return this;
	}

	public BTreeIndexBuilder keyExtractor(KeyExtractor keyExtractor) {
		this.keyExtractor = keyExtractor;
		return this;
	}

	public KeyExtractor getKeyExtractor() {
		return keyExtractor;
	}

	public BTreeIndexBuilder cacheSize(int cacheSize) {
		checkArgument(cacheSize > 0, "cacheSize must be > 0");
		this.cacheSize = cacheSize;
		return this;
	}

	public BTreeIndexBuilder indexRootFolder(File folder) {
		checkNotNull(folder);
		checkArgument(folder.exists(), "indexRootFolder must exist");

		this.indexRootFolder = folder;

		return this;
	}

	public BTreeIndexBuilder indexFolder(String path) {
		return indexRootFolder(new File(path));
	}

	public BTreeIndexBuilder hdfsFilePath(String path) {
		checkNotNull(path);

		// if hdfsFile doesn't start with /, the server name is before the path
		this.hdfsPath = path.replaceAll("^(hdfs://|file:)[^/]*", "");


		return this;
	}


	public Index build() {
		if(primaryIndex)
			return new PrimaryIndex(this);
		else
			return new SecondaryIndex(this);
	}

	public BTreeIndexBuilder keySerializer(FixLengthSerializer ks) {
		this.keySerializer = ks;
		return this;
	}

	public FixLengthSerializer getKeySerializer() {
		return keySerializer;
	}

	public List<Range<K>> getDefaultSearchRanges() {
		if(defaultSearchRanges == null)
			defaultSearchRanges = Lists.newArrayList();
		
		return defaultSearchRanges;
	}

	public BTreeIndexBuilder addDefaultRange(Range r){
		getDefaultSearchRanges().add(r);
		return this;
	}

	public FSDataInputStream getInputStream() {
		return inputStream;
	}

	public BTreeIndexBuilder inputStream(FSDataInputStream inputStream) {
		this.inputStream = inputStream;
		return this;
	}

	public int getSecondaryIndexReadBufferSize() {
		return secondaryIndexReadBufferSize;
	}

	public boolean isPrimaryIndex() {
		return primaryIndex;
	}

	public BTreeIndexBuilder<K, V> primaryIndex() {
		this.primaryIndex = true;
		return this;
	}

	public BTreeIndexBuilder<K, V> secondaryIndex() {
		this.primaryIndex = false;
		return this;
	}

	public BTreeIndexBuilder<K, V> fileSplit(FileSplit fileSplit) {
		this.fileSplit = fileSplit;
		return this;
	}


	int getCacheSize() {
		return cacheSize;
	}

	String getHdfsPath() {
		return hdfsPath;
	}

	File getIndexRootFolder() {
		return indexRootFolder;
	}

	public BTreeIndexBuilder indexFolder(File indexDir) {
		indexRootFolder = indexDir;
		return this;
	}
}
