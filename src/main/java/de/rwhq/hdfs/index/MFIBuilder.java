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

public class MFIBuilder<K,V> {

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
	private int treePageSize = 128 * 1024 * 1024; // default: 128 kb
	private FixLengthSerializer<V, byte[]> valueSerializer;

	public FileSplit getFileSplit() {
		return fileSplit;
	}

	public MFIBuilder<K, V> treePageSize(int treePageSize){
		this.treePageSize = treePageSize;
		return this;
	}

	/**
	 * only required for primary index
	 *
	 * @param valueSerializer
	 * @return this
	 */
	public MFIBuilder<K, V> valueSerializer(FixLengthSerializer<V, byte[]> valueSerializer){
		this.valueSerializer = valueSerializer;
		return this;
	}


	public Configuration getJobConfiguration() {
		return jobConfiguration;
	}

	public MFIBuilder<K, V> defaultSearchRanges(List<Range<K>> ranges){
		this.defaultSearchRanges = ranges;
		return this;
	}

	public MFIBuilder<K, V> jobConfiguration(Configuration jobConfiguration) {
		this.jobConfiguration = jobConfiguration;
		return this;
	}

	public MFIBuilder secondaryIndexReadBufferSize(int secondaryIndexReadBufferSize) {
		this.secondaryIndexReadBufferSize = secondaryIndexReadBufferSize;
		return this;
	}

	public Comparator getComparator() {
		return comparator;
	}

	public MFIBuilder comparator(Comparator comparator) {
		this.comparator = comparator;
		return this;
	}

	public MFIBuilder keyExtractor(KeyExtractor keyExtractor) {
		this.keyExtractor = keyExtractor;
		return this;
	}

	public KeyExtractor getKeyExtractor() {
		return keyExtractor;
	}

	public MFIBuilder cacheSize(int cacheSize) {
		checkArgument(cacheSize > 0, "cacheSize must be > 0");
		this.cacheSize = cacheSize;
		return this;
	}

	public MFIBuilder indexRootFolder(File folder) {
		checkNotNull(folder);
		checkArgument(folder.exists(), "indexRootFolder must exist");

		this.indexRootFolder = folder;

		return this;
	}

	public MFIBuilder indexFolder(String path) {
		return indexRootFolder(new File(path));
	}

	public MFIBuilder hdfsFilePath(String path) {
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

	public MFIBuilder keySerializer(FixLengthSerializer ks) {
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

	public MFIBuilder addDefaultRange(Range r){
		getDefaultSearchRanges().add(r);
		return this;
	}

	public FSDataInputStream getInputStream() {
		return inputStream;
	}

	public MFIBuilder inputStream(FSDataInputStream inputStream) {
		this.inputStream = inputStream;
		return this;
	}

	public int getSecondaryIndexReadBufferSize() {
		return secondaryIndexReadBufferSize;
	}

	public boolean isPrimaryIndex() {
		return primaryIndex;
	}

	public MFIBuilder<K, V> primaryIndex() {
		this.primaryIndex = true;
		return this;
	}

	public MFIBuilder<K, V> secondaryIndex() {
		this.primaryIndex = false;
		return this;
	}

	public MFIBuilder<K, V> fileSplit(FileSplit fileSplit) {
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

	public MFIBuilder indexFolder(File indexDir) {
		indexRootFolder = indexDir;
		return this;
	}

	public int getTreePageSize() {
		return treePageSize;
	}

	public FixLengthSerializer<V, byte[]> getValueSerializer() {
		return valueSerializer;
	}
}
