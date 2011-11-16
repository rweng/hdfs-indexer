package de.rwhq.hdfs.index;

import de.rwhq.comparator.IntegerComparator;
import de.rwhq.serializer.IntegerSerializer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class SecondaryIndexTest {
	@Mock private LineRecordReader recordReader;

	private SecondaryIndex index;
	private final File indexFolder = new File("/tmp/secondaryTest");

	@BeforeMethod
	public void setUp() throws IOException {
		MockitoAnnotations.initMocks(this);

		FileUtils.deleteDirectory(indexFolder);
		indexFolder.mkdir();

		index = (SecondaryIndex) setUpBuilder().build();
	}

	@Factory
	public Object[] createInterfaceTests() {
		return new Object[]{new IndexTest() {
			@Override
			protected AbstractMultiFileIndex getNewIndex() {
				try {
					setUp();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

				when(recordReader.getCurrentValue()).thenAnswer(new Answer<Text>() {
					@Override
					public Text answer(InvocationOnMock invocation) throws Throwable {
						return new Text(IndexTest.map.get(((LineRecordReader) invocation.getMock()).pos));
					}
				});

				return index;
			}
		}};
	}

	private BTreeIndexBuilder setUpBuilder() {
		return new BTreeIndexBuilder()
				.hdfsFilePath("/test/secondaryIndexFile")
				.indexFolder(indexFolder)
				.recordReader(recordReader)
				.keyExtractor(new IntegerCSVExtractor(0, ","))
				.keySerializer(IntegerSerializer.INSTANCE)
				.comparator(IntegerComparator.INSTANCE);
	}
}
