package de.rwhq.hdfs.index;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import static org.fest.assertions.Assertions.assertThat;

public abstract class AbstractMultiFileIndexTest {

	private AbstractMultiFileIndex index;

	@BeforeMethod
	public void setUp() throws IOException {
		index = resetIndex();
	}

	protected abstract AbstractMultiFileIndex resetIndex() throws IOException;

	protected abstract AbstractMultiFileIndex getNewIndex();

	@Test
	public void lockFile() throws IOException {
		addEntriesToIndex();
		assertThat(index.getLockFile()).doesNotExist();
	}

	@Test
	public void ifNotOurLock() throws IOException {
		index.open();
		for (Long l : IndexTest.getSortedMapKeys().subList(0, 3)) {
			index.addLine(IndexTest.map.get(l), l);
		}

		assertThat(index.getLockFile()).exists();

		// this should be ignored, check later with assertProperties()
		AbstractMultiFileIndex index2 = getNewIndex();
		index2.open();
		Long key = IndexTest.getSortedMapKeys().get(3);
		index2.addLine(IndexTest.map.get(key), key);
		index2.close();

		index.close();
	}

	private void addEntriesToIndex() throws IOException {
		index.open();
		for (Long l : IndexTest.getSortedMapKeys().subList(0, 3)) {
			index.addLine(IndexTest.map.get(l), l);
		}
		index.close();
	}

}
