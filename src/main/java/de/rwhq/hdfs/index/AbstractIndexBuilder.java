package de.rwhq.hdfs.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * An abstract class for inheritance by the user.
 * It already defines the methods build() and hdfsFilePath() so the user only has to implement
 * configure().
 */
public abstract class AbstractIndexBuilder implements IndexBuilder {
	private String hdfsFilePath;
	private Configuration conf;
	private FSDataInputStream inputStream;

	@Override
	public IndexBuilder hdfsFilePath(String s) {
		this.hdfsFilePath = s;
		return this;
	}

	@Override
	public Index build() {
		return configure(new BTreeIndexBuilder())
				.hdfsFilePath(hdfsFilePath)
				.jobConfiguration(conf)
				.inputStream(inputStream)
				.build();
	}

	public IndexBuilder jobConfiguration(Configuration conf){
		this.conf = conf;
		return this;
	}

	public IndexBuilder inputStream(FSDataInputStream inputStream){
		this.inputStream = inputStream;
		return this;
	}

	public abstract BTreeIndexBuilder configure(BTreeIndexBuilder builder);
}
