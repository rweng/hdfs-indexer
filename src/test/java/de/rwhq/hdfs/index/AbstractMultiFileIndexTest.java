package de.rwhq.hdfs.index;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileInputStream;
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

	@Test
	public void lockFile() throws IOException {
		addEntriesToIndex();
		assertThat(index.getLockFile()).doesNotExist();
	}

	@Test
	public void propertiesFile() throws IOException {
		addEntriesToIndex();
		assertThat(index.getPropertiesFile()).exists();

		Properties p = new Properties();
		p.loadFromXML(new FileInputStream(index.getPropertiesFile()));

		Enumeration<?> i = p.propertyNames();
		assertThat(i.hasMoreElements()).isTrue();

		String value = (String) p.get(i.nextElement());
		assertThat(i.hasMoreElements()).isFalse();
		assertThat(value).isEqualTo("0;30");
	}

	private void addEntriesToIndex() throws IOException {
		index.open();
		for(Long l : IndexTest.getSortedMapKeys()){
			index.addLine(IndexTest.map.get(l), l);
		}
		index.close();
	}

}
