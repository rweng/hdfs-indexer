package com.freshbourne.hdfs.index;

import org.testng.annotations.Test;

import static org.fest.assertions.Assertions.assertThat;

public class BTreeIndexBuilderTest {
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
