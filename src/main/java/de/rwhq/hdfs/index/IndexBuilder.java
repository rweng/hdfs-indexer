package de.rwhq.hdfs.index;

import com.google.common.collect.Lists;
import de.rwhq.btree.Range;
import de.rwhq.hdfs.index.extractor.KeyExtractor;
import de.rwhq.serializer.FixLengthSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class IndexBuilder<K,V> {

	private int maxPartialsPerSplit = 1;

	public IndexBuilder<K,V> maxPartialsPerSplit(int i){
		maxPartialsPerSplit = i;
		return this;
	}

	public int getMaxPartialsPerSplit() {
		return maxPartialsPerSplit;
	}

	private static enum IndexType {PRIMARY, SECONDARY, NOINDEX}

	private int cacheSize = 1000;
	private File indexRootFolder;
	private FixLengthSerializer<K,byte[]> keySerializer;
	private Comparator<K>  comparator;
	private List<Range<K>> defaultSearchRanges;
	private FSDataInputStream inputStream;
	private KeyExtractor keyExtractor;
	private Configuration jobConfiguration;
	private FileSplit fileSplit;
	
	private IndexType indexType = IndexType.NOINDEX;

	private int secondaryIndexReadBufferSize = 500;
	private int treePageSize = 128 * 1024; // default: 128 kb
	private FixLengthSerializer<V, byte[]> valueSerializer;

	public FileSplit getFileSplit() {
		return fileSplit;
	}

	public IndexBuilder<K, V> treePageSize(int treePageSize){
		this.treePageSize = treePageSize;
		return this;
	}

	/**
	 * only required for primary index
	 *
	 * @param valueSerializer
	 * @return this
	 */
	public IndexBuilder<K, V> valueSerializer(FixLengthSerializer<V, byte[]> valueSerializer){
		this.valueSerializer = valueSerializer;
		return this;
	}


	public Configuration getJobConfiguration() {
		return jobConfiguration;
	}

	public IndexBuilder<K, V> defaultSearchRanges(List<Range<K>> ranges){
		this.defaultSearchRanges = ranges;
		return this;
	}

	public IndexBuilder<K, V> jobConfiguration(Configuration jobConfiguration) {
		this.jobConfiguration = jobConfiguration;
		return this;
	}

	public IndexBuilder secondaryIndexReadBufferSize(int secondaryIndexReadBufferSize) {
		this.secondaryIndexReadBufferSize = secondaryIndexReadBufferSize;
		return this;
	}

	public Comparator getComparator() {
		return comparator;
	}

	public IndexBuilder comparator(Comparator comparator) {
		this.comparator = comparator;
		return this;
	}

	public IndexBuilder keyExtractor(KeyExtractor keyExtractor) {
		this.keyExtractor = keyExtractor;
		return this;
	}

	public KeyExtractor getKeyExtractor() {
		return keyExtractor;
	}

	public IndexBuilder cacheSize(int cacheSize) {
		checkArgument(cacheSize > 0, "cacheSize must be > 0");
		this.cacheSize = cacheSize;
		return this;
	}

	public IndexBuilder indexRootFolder(File folder) {
		checkNotNull(folder);
		checkArgument(folder.exists(), "indexRootFolder must exist");

		this.indexRootFolder = folder;

		return this;
	}

	public IndexBuilder indexFolder(String path) {
		return indexRootFolder(new File(path));
	}


	public Index build() {
		switch (indexType){
			case NOINDEX: return new NoIndex(this);
			case PRIMARY: return new PrimaryIndex(this);
			case SECONDARY: return new SecondaryIndex(this);
			default: throw new IllegalStateException("indexType unknown");
		}
	}

	public IndexBuilder keySerializer(FixLengthSerializer ks) {
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

	public IndexBuilder addDefaultRange(Range r){
		getDefaultSearchRanges().add(r);
		return this;
	}

	public FSDataInputStream getInputStream() {
		return inputStream;
	}

	public IndexBuilder inputStream(FSDataInputStream inputStream) {
		this.inputStream = inputStream;
		return this;
	}

	public int getSecondaryIndexReadBufferSize() {
		return secondaryIndexReadBufferSize;
	}


	public IndexBuilder<K, V> primaryIndex() {
		this.indexType = IndexType.PRIMARY;
		return this;
	}

	public IndexBuilder<K, V> secondaryIndex() {
		this.indexType = IndexType.SECONDARY;
		return this;
	}

	public IndexBuilder<K, V> fileSplit(FileSplit fileSplit) {
		this.fileSplit = fileSplit;
		return this;
	}


	int getCacheSize() {
		return cacheSize;
	}

	File getIndexRootFolder() {
		return indexRootFolder;
	}

	public IndexBuilder indexFolder(File indexDir) {
		indexRootFolder = indexDir;
		return this;
	}

	public int getTreePageSize() {
		return treePageSize;
	}

	public FixLengthSerializer<V, byte[]> getValueSerializer() {
		return valueSerializer;
	}

	public IndexBuilder noIndex() {
		indexType = IndexType.NOINDEX;
		return this;
	}
}
