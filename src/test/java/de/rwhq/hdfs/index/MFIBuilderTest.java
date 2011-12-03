package de.rwhq.hdfs.index;

import de.rwhq.comparator.IntegerComparator;
import de.rwhq.serializer.IntegerSerializer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;

public class MFIBuilderTest {
	private static final File indexFolder = new File("/tmp/index");
	@Mock private FileSplit fileSplit;

	@Before
	public void setUp() throws IOException {
		MockitoAnnotations.initMocks(this);
		FileUtils.deleteDirectory(indexFolder);
		indexFolder.mkdir();
	}

	@Test(expected = NullPointerException.class)
	public void exceptionIfNoHdfsPath() {
		new MFIBuilder().build();
	}


	private MFIBuilder setUpBuilder() {
		return new MFIBuilder()
				.indexFolder("/tmp/index")
				.hdfsFilePath("hdfs://localhost/test/bla")
				.keyExtractor(new IntegerCSVExtractor(0, "( |\\t)+"))
				.keySerializer(IntegerSerializer.INSTANCE)
				.fileSplit(fileSplit)
				.comparator(IntegerComparator.INSTANCE);

	}


	@Test
	public void validCase() {
		AbstractMultiFileIndex build = (AbstractMultiFileIndex) setUpBuilder().build();
		assertThat(build.getIndexFolder().getAbsolutePath()).isEqualTo("/tmp/index/test/bla");
	}


	@Test
	public void strippingOfProtocol() {
		AbstractMultiFileIndex build = (AbstractMultiFileIndex) setUpBuilder().build();
		assertThat(build.getIndexFolder().getAbsolutePath()).isEqualTo("/tmp/index/test/bla");

		build = (AbstractMultiFileIndex) setUpBuilder().hdfsFilePath("hdfs:///test/bla").build();
		assertThat(build.getIndexFolder().getAbsolutePath()).isEqualTo("/tmp/index/test/bla");
	}
}
