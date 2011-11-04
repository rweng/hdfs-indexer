package com.freshbourne.hdfs.index;

import org.apache.log4j.Logger;

public class IntegerCSVExtractor implements KeyExtractor<Integer> {

	private String delimiter;
	private int    column;

	private static Logger LOG = Logger.getLogger(IntegerCSVExtractor.class);


	public IntegerCSVExtractor(int column,
	                            String delimiter) {
		this.column = column;
		this.delimiter = delimiter;

		if (LOG.isDebugEnabled()) {
			LOG.debug("delimiter = '" + this.delimiter + "'");
			LOG.debug("column = " + this.column);
		}
	}

	@Override public Integer extract(String line) {
		String[] splits = line.split(delimiter);
		if (LOG.isDebugEnabled())
			LOG.debug("trying to transform key: '" + splits[column] + "'");
		return Integer.parseInt(splits[column]);
	}
}
