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
		BTreeIndex build = new BTreeIndexBuilder().hdfsFilePath("/test/bla").build();
		assertThat(build.getHdfsFile()).isNotEmpty();
	}
}
