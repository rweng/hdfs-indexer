package de.rwhq.hdfs.index;

import com.google.common.base.Objects;
import com.google.common.collect.ObjectArrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;

public class IntegerCSVExtractor implements KeyExtractor<Integer> {

	private String delimiter;
	private int    column;

	private static Log LOG = LogFactory.getLog(IntegerCSVExtractor.class);


	public IntegerCSVExtractor(int column,
	                            String delimiter) {
		this.column = column;
		this.delimiter = delimiter;

		if (LOG.isDebugEnabled()) {
			LOG.debug("delimiter = '" + this.delimiter + "'");
			LOG.debug("column = " + this.column);
		}
	}

	@Override public Integer extract(String line) throws ExtractionException {
		try{
		String[] splits = line.split(delimiter);
//		if (LOG.isDebugEnabled()){
//			LOG.debug("trying to transform key: '" + splits[column] + "'");
//			LOG.debug("all splits: " + Arrays.toString(splits));
//		}
		return Integer.parseInt(splits[column]);
		} catch (Exception e){
			throw new ExtractionException(e);
		}
	}

	@Override public String getId() {
		return String.valueOf(column);
	}
}
