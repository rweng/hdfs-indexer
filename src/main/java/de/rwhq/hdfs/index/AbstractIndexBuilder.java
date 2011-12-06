package de.rwhq.hdfs.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * An abstract class for inheritance by the user.
 */
public abstract class AbstractIndexBuilder extends IndexBuilder {
	
	@Override
	public Index build() {
		configure(this);
		return super.build();
	}

	public abstract IndexBuilder configure(IndexBuilder builder);
}
