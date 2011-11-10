package de.rwhq.hdfs.index;

/**
 * Exception thrown by {@code KeyExtractor}
 */
public class ExtractionException extends Exception {
	ExtractionException(){super();}

	ExtractionException(String msg){
		super(msg);
	}

	ExtractionException(Exception e){
		super(e);
	}
}