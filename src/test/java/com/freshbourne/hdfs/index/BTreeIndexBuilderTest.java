package com.freshbourne.hdfs.index;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;

public class BTreeIndexBuilderTest {
	private static final File indexFolder = new File("/tmp/index");

	@BeforeMethod
	public void setUp() throws IOException {
		FileUtils.deleteDirectory(indexFolder);
		indexFolder.mkdir();
	}

	@Test(expectedExceptions = IllegalStateException.class)
	public void exceptionIfNoHdfsPath(){
		new BTreeIndexBuilder().build();
	}

	@Test
	public void validCase(){
		BTreeIndex build = new BTreeIndexBuilder().indexFolder("/tmp/index").hdfsFilePath("/test/bla").build();
		assertThat(build.getIndexFolder().getAbsolutePath()).isEqualTo("/tmp/index/test/bla");
	}

	@Test
	public void strippingOfProtocol(){
		BTreeIndex build = new BTreeIndexBuilder().indexFolder("/tmp/index").hdfsFilePath("hdfs://localhost/test/bla").build();
		assertThat(build.getIndexFolder().getAbsolutePath()).isEqualTo("/tmp/index/test/bla");

		build = new BTreeIndexBuilder().indexFolder("/tmp/index").hdfsFilePath("hdfs:///test/bla").build();
		assertThat(build.getIndexFolder().getAbsolutePath()).isEqualTo("/tmp/index/test/bla");
	}
}
