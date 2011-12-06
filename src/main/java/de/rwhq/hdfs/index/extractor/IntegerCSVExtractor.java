package de.rwhq.hdfs.index.extractor;

import com.google.common.base.Objects;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IntegerCSVExtractor implements KeyExtractor<Integer> {

	private String delimiter;
	private int    column;

	private static Log LOG = LogFactory.getLog(IntegerCSVExtractor.class);


	public IntegerCSVExtractor(int column,
	                           String delimiter) {
		this.column = column;
		this.delimiter = delimiter;

		if (LOG.isDebugEnabled()) {
			LOG.debug("constructed: " + this);
		}
	}

	@Override
	public Integer extract(String line) throws ExtractionException {
		try {
			String[] splits = line.split(delimiter);
			return Integer.parseInt(splits[column]);
		} catch (Exception e) {
			throw new ExtractionException(e);
		}
	}

	@Override
	public String getId() {
		return String.valueOf(column);
	}

	@Override
	public String toString(){
		return Objects.toStringHelper(this)
				.add("delimiter", delimiter)
				.add("column", column)
				.toString();
	}
}
