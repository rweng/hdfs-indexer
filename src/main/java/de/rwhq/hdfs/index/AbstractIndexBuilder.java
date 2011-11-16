package de.rwhq.hdfs.index;

/**
 * An abstract class for inheritance by the user.
 * It already defines the methods build() and hdfsFilePath() so the user only has to implement
 * configure().
 */
public abstract class AbstractIndexBuilder implements IndexBuilder {
	private String hdfsFilePath;
	private IndexedRecordReader recordReader;

	@Override
	public IndexBuilder hdfsFilePath(String s) {
		this.hdfsFilePath = s;
		return this;
	}

	@Override
	public Index build() {
		return configure(new BTreeIndexBuilder())
				.hdfsFilePath(hdfsFilePath)
				.recordReader(recordReader)
				.build();
	}


	public IndexBuilder recordReader(IndexedRecordReader indexedRecordReader){
		this.recordReader = indexedRecordReader;
		return this;
	}

	public abstract BTreeIndexBuilder configure(BTreeIndexBuilder builder);
}
