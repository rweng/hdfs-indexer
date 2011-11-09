package de.rwhq.hdfs.index;

import de.rwhq.comparator.IntegerComparator;
import de.rwhq.serializer.IntegerSerializer;
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

	@Test(expectedExceptions = NullPointerException.class)
	public void exceptionIfNoHdfsPath(){
		new BTreeIndexBuilder().build();
	}

	@Test
	public void validCase(){
		BTreeIndex build = setUpBuilder().build();
		assertThat(build.getIndexFolder().getAbsolutePath()).isEqualTo("/tmp/index/test/bla");
	}

	private BTreeIndexBuilder setUpBuilder(){
		return new BTreeIndexBuilder()
				.indexFolder("/tmp/index")
				.hdfsFilePath("hdfs://localhost/test/bla")
				.keyExtractor(new IntegerCSVExtractor(0, "( |\\t)+"))
				.keySerializer(IntegerSerializer.INSTANCE)
				.comparator(IntegerComparator.INSTANCE);
	}

	@Test
	public void strippingOfProtocol(){
		BTreeIndex build = setUpBuilder().build();
		assertThat(build.getIndexFolder().getAbsolutePath()).isEqualTo("/tmp/index/test/bla");

		build = setUpBuilder().hdfsFilePath("hdfs:///test/bla").build();
		assertThat(build.getIndexFolder().getAbsolutePath()).isEqualTo("/tmp/index/test/bla");
	}
}
